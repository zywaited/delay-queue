package timing_wheel

import (
	"errors"
	"sync/atomic"

	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
)

const ServerName timer.ScannerType = "timing-wheel-server"

type (
	serverConfig struct {
		dt DecorateTask
	}

	ServerConfigOption func(*serverConfig)
)

func ServerConfigWithTaskProto(dt DecorateTask) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.dt = dt
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
	s.tw.Add(s.c.dt.Decorate(t))
	return nil
}

func (s *server) Remove(t task.Task) error {
	switch atomic.LoadInt32(&s.status) {
	case role.StatusRunning, role.StatusGraceFulST:
		s.tw.Remove(s.c.dt.Decorate(t))
	}
	return nil
}

func (s *server) Running() bool {
	return atomic.LoadInt32(&s.status) == role.StatusRunning && s.tw.running()
}

func (s *server) Stopped() bool {
	return atomic.LoadInt32(&s.status) != role.StatusRunning || !s.tw.running()
}
