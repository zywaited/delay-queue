package reload

import (
	"sync"
	"time"

	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
)

type (
	server struct {
		reload       role.GenerateLoseTask
		logger       system.Logger
		reloadGN     int
		reloadScale  time.Duration
		reloadPerNum int
		runner       task.Runner

		sr timer.Scanner
	}

	ServerConfigOption func(*server)
)

func ServerConfigWithReload(r role.GenerateLoseTask) ServerConfigOption {
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
		sc.reloadGN = gn
	}
}
func ServerConfigWithReloadScale(scale time.Duration) ServerConfigOption {
	return func(sc *server) {
		sc.reloadScale = scale
	}
}

func ServerConfigWithReloadPerNum(limit int) ServerConfigOption {
	return func(sc *server) {
		sc.reloadPerNum = limit
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

// 默认加载数量
const (
	defaultReloadGN     = 1
	defaultReloadPerNum = 50
)

func NewServer(opts ...ServerConfigOption) *server {
	s := &server{reloadGN: defaultReloadGN, reloadPerNum: defaultReloadPerNum}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *server) Run() error {
	go s.run()
	return nil
}

func (s *server) Stop(t role.StopType) error {
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
	l, err := s.reload.Len()
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
		s.logger.Info("reload latest task start")
	}
	maxNum := l/s.reloadGN + 1
	wg := &sync.WaitGroup{}
	wg.Add(s.reloadGN)
	for index := 0; index < s.reloadGN; index++ {
		go func(index int) {
			defer wg.Done()
			s.task(index, maxNum, s.reloadPerNum)
		}(index)
	}
	wg.Wait()
	if s.logger != nil {
		s.logger.Info("reload latest task end")
	}
}

func (s *server) task(index, maxNum, limit int) {
	defer func() {
		rerr := recover()
		if rerr == nil || s.logger == nil {
			return
		}
		s.logger.Infof("reload latest task err: %v, stack: %s", rerr, system.Stack())
	}()
	offset := maxNum * index
	fetchedNum := 0
	maxTimeout := 10
	currentTimeout := 0
	for {
		if maxNum-fetchedNum < limit {
			limit = maxNum - fetchedNum
		}
		if limit <= 0 {
			break
		}
		ts, err := s.reload.Reload(int64(offset), int64(limit))
		if err != nil {
			if s.logger == nil {
				return
			}
			s.logger.Infof("reload latest task err: %v", err)
			if currentTimeout < maxTimeout {
				currentTimeout++
			}
			time.Sleep(s.reloadScale << currentTimeout)
			continue
		}
		if len(ts) == 0 {
			return
		}
		offset += limit
		fetchedNum += limit
		for _, t := range ts {
			rt, ok := t.(task.RunnerTask)
			if !ok {
				continue
			}
			rt.InitRunner(s.runner)
			err = s.sr.Add(t)
			if err == nil || s.logger == nil {
				continue
			}
			s.logger.Infof("reload latest task[%s] err: %v", t.Uid(), err)
		}
		time.Sleep(s.reloadScale)
		currentTimeout = 0
	}
}
