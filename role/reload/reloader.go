package reload

import (
	"context"
	"errors"
	"time"

	pkerr "github.com/pkg/errors"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/limiter"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
)

var errEmptyTask = errors.New("no valid task")

type (
	server struct {
		gls          role.GenerateLoseStore
		reload       role.GeneratePool
		ds           role.DataStore
		logger       system.Logger
		runner       task.Runner
		sr           timer.Scanner
		gp           limiter.Pool
		reloadGN     int
		reloadPerNum int
		reloadScale  time.Duration
		st           int64
		et           int64
		wg           chan struct{}
		rg           chan role.GenerateLoseTask
	}

	ServerConfigOption func(*server)
)

func ServerConfigWithReload(r role.GeneratePool) ServerConfigOption {
	return func(sc *server) {
		sc.reload = r
	}
}

func ServerConfigWithLogger(logger system.Logger) ServerConfigOption {
	return func(sc *server) {
		sc.logger = logger
	}
}

func ServerConfigWithReloadGN(gn int) ServerConfigOption {
	return func(sc *server) {
		if gn > 0 {
			sc.reloadGN = gn
		}
	}
}
func ServerConfigWithReloadScale(scale time.Duration) ServerConfigOption {
	return func(sc *server) {
		sc.reloadScale = scale
	}
}

func ServerConfigWithReloadPerNum(limit int) ServerConfigOption {
	return func(sc *server) {
		if limit > 0 {
			sc.reloadPerNum = limit
		}
	}
}

func ServerConfigWithRunner(runner task.Runner) ServerConfigOption {
	return func(sc *server) {
		sc.runner = runner
	}
}

func ServerConfigWithTimer(sr timer.Scanner) ServerConfigOption {
	return func(sc *server) {
		sc.sr = sr
	}
}
func ServerConfigWithStore(ds role.DataStore) ServerConfigOption {
	return func(sc *server) {
		sc.ds = ds
	}
}

func ServerConfigWithGP(gp limiter.Pool) ServerConfigOption {
	return func(s *server) {
		s.gp = gp
	}
}

func ServerConfigWithGLS(gls role.GenerateLoseStore) ServerConfigOption {
	return func(s *server) {
		s.gls = gls
	}
}

// 默认加载数量
const (
	defaultReloadGN     = 1
	defaultReloadPerNum = 50
)

func NewServer(opts ...ServerConfigOption) *server {
	s := &server{
		reloadGN:     defaultReloadGN,
		reloadPerNum: defaultReloadPerNum,
		et:           time.Now().UnixNano(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *server) Run() error {
	go s.run()
	return nil
}

func (s *server) Stop(_ role.StopType) error {
	return nil
}

func (s *server) init() {
	s.wg = make(chan struct{}, s.reloadGN)
	s.rg = make(chan role.GenerateLoseTask, s.reloadGN)
	for index := 0; index < s.reloadGN; index++ {
		s.wg <- struct{}{}
	}
	s.st = 0
}

// 直到所有的数据都被写入
func (s *server) run() {
	defer func() {
		rerr := recover()
		if rerr == nil || s.logger == nil {
			return
		}
		s.logger.Infof("reload latest task err: %v, stack: %s", rerr, system.Stack())
	}()
	if s.reload == nil {
		return
	}
	l, err := s.gls.ReadyNum(s.st, s.et)
	if err != nil {
		if s.logger != nil {
			s.logger.Infof("reload latest task err: %v", err)
		}
		return
	}
	if l == 0 {
		return
	}
	if srt, ok := s.sr.(role.LauncherStatus); ok {
		// 这里需要等待启动
		for !srt.Running() {
			time.Sleep(s.reloadScale)
		}
	}
	if s.logger != nil {
		s.logger.Infof("reload latest task start: %d", l)
	}
	s.init()
	s.reloadTasks()
	if s.logger != nil {
		s.logger.Info("reload latest task end")
	}
}

func (s *server) reloadTasks() {
	maxTimeout := 10
	currentTimeout := 0
	quitNum := 0
	for {
		tk, err := s.getTask()
		if err != nil && err != errEmptyTask {
			if s.logger != nil {
				s.logger.Infof("get task failed: %v", err)
			}
			if currentTimeout < maxTimeout {
				currentTimeout++
			}
			time.Sleep(s.reloadScale << currentTimeout)
			continue
		}
		if err == errEmptyTask {
			// over
			if quitNum >= s.reloadGN {
				break
			}
			<-s.wg
			quitNum++
			continue
		}
		s.runTask(tk)
		if quitNum > 0 {
			quitNum--
		}
		time.Sleep(s.reloadScale)
	}
}

func (s *server) getChanTask() (t role.GenerateLoseTask, err error) {
	select {
	case t = <-s.rg:
	default:
		err = errEmptyTask
	}
	return
}

func (s *server) getRemoteTask() (t role.GenerateLoseTask, err error) {
	if s.st > s.et {
		err = errEmptyTask
		return
	}
	et, fer := s.gls.NextReady(s.st, s.et, int64(s.reloadPerNum))
	if fer != nil {
		if xerror.IsXCode(fer, xcode.DBRecordNotFound) {
			err = errEmptyTask
			s.st = s.et + 1
			return
		}
		err = pkerr.WithMessage(fer, "get task failed")
		return
	}
	t = s.reload.Generate(s.st, et, int64(s.reloadPerNum))
	s.st = et + 1
	return
}

func (s *server) getTask() (t role.GenerateLoseTask, err error) {
	t, _ = s.getChanTask()
	if t != nil {
		return
	}
	return s.getRemoteTask()
}

func (s *server) runTask(t role.GenerateLoseTask) {
	<-s.wg
	_ = s.gp.Submit(context.Background(), func() {
		defer func() {
			s.wg <- struct{}{}
		}()
		ts, err := t.Reload()
		if err != nil {
			if s.logger != nil {
				s.logger.Infof("reload latest task err: %v", err)
			}
			// reset
			s.rg <- t
			return
		}
		if len(ts) == 0 {
			s.reload.Release(t)
			return
		}
		for _, tk := range ts {
			rt, ok := tk.(task.RunnerTask)
			if !ok {
				continue
			}
			rt.InitRunner(s.runner)
			err = s.sr.Add(tk)
			if err == nil || s.logger == nil {
				continue
			}
			s.logger.Infof("reload latest task[%s] err: %v", tk.Uid(), err)
		}
		if s.logger != nil {
			s.logger.Infof("reload task: %d", len(ts))
		}
		// 这里轮换是为了不占用协程时间太久
		s.rg <- t
	})
}
