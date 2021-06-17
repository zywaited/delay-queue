package reload

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	pkerr "github.com/pkg/errors"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
	"github.com/zywaited/go-common/limiter"
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
		maxCheckTime time.Duration
		st           int64
		lt           int64
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

func ServerConfigWithMaxCheckTime(t time.Duration) ServerConfigOption {
	return func(sc *server) {
		sc.maxCheckTime = t
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

func ServerConfigWithST(st int64) ServerConfigOption {
	return func(s *server) {
		s.st = st
	}
}

func ServerConfigWithET(et int64) ServerConfigOption {
	return func(s *server) {
		s.et = et
	}
}

// 默认加载数量
const (
	defaultReloadGN       = 1
	defaultReloadPerNum   = 50
	defaultReloadOverTime = time.Minute * 3
)

func NewServer(opts ...ServerConfigOption) *server {
	s := &server{
		reloadGN:     defaultReloadGN,
		reloadPerNum: defaultReloadPerNum,
		maxCheckTime: defaultReloadOverTime,
		et:           time.Now().UnixNano(), // 这里不应该是时间戳了
	}
	for _, opt := range opts {
		opt(s)
	}
	s.wg = make(chan struct{}, s.reloadGN)
	s.rg = make(chan role.GenerateLoseTask, s.reloadGN)
	s.st = 0
	return s
}

func (s *server) Run() error {
	go s.run()
	return nil
}

func (s *server) Stop(_ role.StopType) error {
	return nil
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
	s.reloadTasks()
	if s.logger != nil {
		s.logger.Info("reload latest task end")
	}
}

func (s *server) reloadTasks() {
	var tc <-chan time.Time
	maxTimeout := 5
	currentTimeout := 0
	quitNum := s.reloadGN
	sleep := func(reset bool) {
		if !reset && currentTimeout < maxTimeout {
			currentTimeout++
		}
		if reset {
			currentTimeout = 0
		}
		time.Sleep(s.reloadScale << currentTimeout)
	}
	for {
		tk, err := s.getTask()
		if err != nil && err != errEmptyTask {
			if s.logger != nil {
				s.logger.Infof("get task failed: %v", err)
			}
			sleep(false)
			continue
		}
		if err == errEmptyTask {
			if quitNum < s.reloadGN {
				<-s.wg
				quitNum++
				continue
			}
			if tc == nil && s.maxCheckTime > 0 {
				tc = time.After(s.maxCheckTime)
			}
			// over
			select {
			case <-tc:
				return
			default:
			}
			sleep(false)
			s.st = s.lt
			continue
		}
		if quitNum > 0 {
			s.wg <- struct{}{}
			quitNum--
		}
		s.runTask(tk)
		sleep(true)
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
		// 记录最大的值
		maxLast := t.Next()
		for {
			currentLast := atomic.LoadInt64(&s.lt)
			if maxLast <= currentLast {
				break
			}
			if atomic.CompareAndSwapInt64(&s.lt, currentLast, maxLast) {
				break
			}
		}
		if t.Valid() {
			// 这里轮换是为了不占用协程时间太久
			s.rg <- t
		}
	})
}
