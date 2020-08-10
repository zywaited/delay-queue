package timing_wheel

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
)

const ServerName timer.ScannerType = "timing-wheel-server"

type (
	serverConfig struct {
		factory      task.PoolFactory
		reload       role.GenerateLoseTask
		logger       system.Logger
		reloadGN     int
		reloadScale  time.Duration
		reloadPerNum int
	}

	ServerConfigOption func(*serverConfig)
)

func ServerConfigWithFactory(factory task.PoolFactory) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.factory = factory
	}
}

func ServerConfigWithReload(r role.GenerateLoseTask) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.reload = r
	}
}

func ServerConfigWithLogger(logger system.Logger) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.logger = logger
	}
}

func ServerConfigWithReloadGN(gn int) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.reloadGN = gn
	}
}
func ServerConfigWithReloadScale(scale time.Duration) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.reloadScale = scale
	}
}

func ServerConfigWithReloadPerNum(limit int) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.reloadPerNum = limit
	}
}

func NewServerConfig(opts ...ServerConfigOption) *serverConfig {
	c := &serverConfig{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type server struct {
	c      *serverConfig
	tw     *TimeWheel
	p      task.Pool
	status int32
}

func NewServer(c *serverConfig, opts ...Option) *server {
	s := &server{
		c:      c,
		tw:     NewTimeWheel(opts...),
		status: role.StatusInitialized,
	}
	if c == nil {
		return s
	}
	if c.factory != nil {
		s.p = c.factory.NewPool(newBindJobTask)
	}
	return s
}

func (s *server) Name() timer.ScannerType {
	return ServerName
}

func (s *server) Run() error {
	// CAS
	for {
		cv := atomic.LoadInt32(&s.status)
		if cv == role.StatusRunning {
			return nil
		}
		if !atomic.CompareAndSwapInt32(&s.status, cv, role.StatusRunning) {
			continue
		}
		if cv == role.StatusForceSTW || cv == role.StatusInitialized {
			s.tw.Run()
			go s.reload()
		}
		return nil
	}
}

func (s *server) Stop(t role.StopType) error {
	switch t {
	case role.GraceFulST:
		// 只能从启动状态停止
		atomic.CompareAndSwapInt32(&s.status, role.StatusRunning, role.StatusGraceFulST)
	case role.ForceSTW:
		// CAS
		for {
			cv := atomic.LoadInt32(&s.status)
			if cv == role.StatusForceSTW {
				return nil
			}
			if !atomic.CompareAndSwapInt32(&s.status, cv, role.StatusForceSTW) {
				continue
			}
			if cv == role.StatusRunning || cv == role.StatusGraceFulST {
				s.tw.Stop()
			}
			return nil
		}
	}
	return nil
}

func (s *server) Add(t task.Task) error {
	if atomic.LoadInt32(&s.status) != role.StatusRunning {
		return errors.New("server not run")
	}
	s.tw.Add(s.decorateTask(t))
	return nil
}

func (s *server) Remove(t task.Task) error {
	switch atomic.LoadInt32(&s.status) {
	case role.StatusRunning, role.StatusGraceFulST:
		s.tw.Remove(s.decorateTask(t))
	}
	return nil
}

func (s *server) decorateTask(t task.Task) Task {
	j := (Task)(nil)
	if s.p == nil {
		// 池化不存在也可以执行
		j = NewBindJob()
	} else {
		j = s.p.NewTask().(Task)
		if pj, ok := j.(task.PoolTask); ok {
			pj.Bind(s.p)
		}
	}
	if jp, ok := j.(poolJob); ok {
		jp.Extend(t)
	}
	return j
}

// 直到所有的数据都被写入
func (s *server) reload() {
	if s.c.reload == nil {
		return
	}
	l, err := s.c.reload.Len()
	if err != nil {
		if s.c.logger != nil {
			s.c.logger.Infof("reload latest task err: %v", err)
		}
		return
	}
	if l == 0 {
		return
	}
	if s.c.logger != nil {
		s.c.logger.Info("reload latest task")
	}
	gn := l/s.c.reloadGN + 1
	wg := &sync.WaitGroup{}
	wg.Add(s.c.reloadGN)
	fn := func(index int) {
		defer func() {
			rerr := recover()
			wg.Done()
			if rerr == nil || s.c.logger == nil {
				return
			}
			s.c.logger.Infof("reload latest task err: %v, stack: %s", rerr, system.Stack())
		}()
		offset := gn * index
		limit := s.c.reloadPerNum
		num := 0
		maxTimeout := 10
		currentTimeout := 0
		for {
			if gn-num < limit {
				limit = gn - num
			}
			if limit == 0 {
				break
			}
			ts, err := s.c.reload.Reload(int64(offset), int64(limit))
			if err != nil {
				if s.c.logger == nil {
					return
				}
				s.c.logger.Infof("reload latest task err: %v", err)
				if currentTimeout < maxTimeout {
					currentTimeout++
				}
				time.Sleep(s.c.reloadScale << currentTimeout)
				continue
			}
			if len(ts) == 0 {
				return
			}
			offset += limit
			num += len(ts)
			for _, t := range ts {
				err = s.Add(t)
				if err == nil || s.c.logger == nil {
					continue
				}
				s.c.logger.Infof("reload latest task[%s] err: %v", t.Uid(), err)
			}
			time.Sleep(s.c.reloadScale)
			currentTimeout = 0
		}
	}
	for index := 0; index < s.c.reloadGN; index++ {
		go fn(index)
	}
	wg.Wait()
}
