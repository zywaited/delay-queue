package sorted

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
)

const ServerName timer.ScannerType = "sorted-server"

type server struct {
	c *serverConfig

	store  task.Store
	ticker *time.Ticker
	status int32

	addTaskChannel    chan task.Task
	removeTaskChannel chan task.Task
	stopChannel       chan bool
}

func NewServer(opts ...ServerConfigOption) *server {
	c := &serverConfig{}
	for _, opt := range opts {
		opt(c)
	}
	return &server{
		c:                 c,
		store:             c.ns(),
		status:            role.StatusInitialized,
		addTaskChannel:    make(chan task.Task, 1),
		removeTaskChannel: make(chan task.Task, 1),
		stopChannel:       make(chan bool),
	}
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
			go s.run()
		}
		return nil
	}
}

func (s *server) Stop(t role.StopType) error {
	switch t {
	case role.GraceFulST:
		return s.graceFulST()
	case role.ForceSTW:
		return s.forceST()
	}
	return nil
}

func (s *server) Add(t task.Task) error {
	if t.Uid() == "" {
		return nil
	}
	s.addTaskChannel <- t
	return nil
}

func (s *server) Remove(t task.Task) error {
	if t.Uid() == "" {
		return nil
	}
	s.removeTaskChannel <- t
	return nil
}

func (s *server) graceFulST() error {
	// 只能从启动状态停止
	if !atomic.CompareAndSwapInt32(&s.status, role.StatusRunning, role.StatusGraceFulST) {
		return fmt.Errorf("server[%s] can't graceFulST: not running", string(ServerName))
	}
	// 如果数据已经完结，那么退出即可
	go func() {
		// 沿用ticker(不用太频繁)
		tr := time.NewTicker(s.c.scale << 1)
		defer func() {
			_ = recover()
			tr.Stop()
		}()
		for {
			<-tr.C
			if atomic.LoadInt32(&s.status) != role.StatusGraceFulST {
				return
			}
			if s.store.Len() > 0 {
				continue
			}
			// 这里不考虑并发启动问题
			_ = s.forceST()
			return
		}
	}()
	return nil
}

func (s *server) forceST() error {
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
			s.stopChannel <- true
		}
		return nil
	}
}

func (s *server) run() {
	s.ticker = time.NewTicker(s.c.scale)
	defer func() {
		if err := recover(); err != nil && s.c.logger != nil {
			s.c.logger.Error("sorted server panic: %v, stack: %s", err, system.Stack())
		}
		if s.c.logger != nil {
			s.c.logger.Info("sorted server stop")
		}
		s.ticker.Stop()
	}()
	for {
		select {
		case <-s.ticker.C:
			// 顺序执行减少重复数据
			// 因为这里的store是公用的
			s.scanTask()
		case t := <-s.addTaskChannel:
			s.addTask(t)
		case t := <-s.removeTaskChannel:
			s.removeTask(t)
		case <-s.stopChannel:
			return
		}
	}
}

func (s *server) removeTask(t task.Task) {
	var err error
	pt, ok := t.(task.Result)
	defer func() {
		if rerr := recover(); rerr != nil {
			if s.c.logger != nil {
				s.c.logger.Errorf(
					"task: %s remove error: %v, stack: %s",
					t.Uid(), rerr, system.Stack(),
				)
			}
			err = errors.New("sorted-server remove task panic")
		}
		if ok {
			pt.CloseResult(err)
		}
	}()
	if err = s.store.Remove(t.Uid()); err != nil && s.c.logger != nil {
		s.c.logger.Infof("task: %s remove error: %s", t.Uid(), err.Error())
	}
}

func (s *server) addTask(t task.Task) {
	var err error
	pt, ok := t.(task.Result)
	defer func() {
		if rerr := recover(); rerr != nil {
			if s.c.logger != nil {
				s.c.logger.Errorf(
					"task: %s add error: %v, stack: %s",
					t.Uid(), rerr, system.Stack(),
				)
			}
			err = errors.New("sorted-server add task panic")
		}
		if ok {
			pt.CloseResult(err)
		}
	}()
	if err = s.store.Add(t); err != nil && s.c.logger != nil {
		s.c.logger.Infof("task: %s add error: %s", t.Uid(), err.Error())
	}
}

func (s *server) scanTask() {
	iter := s.store.Iter()
	defer iter.Release()
	var uids []string
	_, mr := s.store.(task.StoreRemover)
	for iter.Next() {
		t := iter.Scan()
		_ = s.c.gp.Submit(context.Background(), func() {
			defer func() {
				if err := recover(); err != nil && s.c.logger != nil {
					s.c.logger.Errorf("task: %s exec error: %v, stack: %s", t.Uid(), err, system.Stack())
				}
				t.Release()
			}()
			err := t.Run()
			if err != nil {
				if s.c.logger != nil {
					s.c.logger.Infof("task: %s exec error: %s", t.Uid(), err.Error())
				}
				return
			}
			if mr {
				return
			}
			if pt, ok := t.(task.Result); ok {
				pt.InitResult()
				_ = s.Remove(t)
				_ = pt.WaitResult()
			}
		})
		if mr {
			uids = append(uids, t.Uid())
		}
	}
	if len(uids) == 0 {
		return
	}
	if err := s.store.(task.StoreRemover).MultiRemove(uids); err != nil && s.c.logger != nil {
		s.c.logger.Infof("task: %v remove error: %s (重复处理)", uids, err.Error())
	}
}

func (s *server) Running() bool {
	return atomic.LoadInt32(&s.status) == role.StatusRunning
}

func (s *server) Stopped() bool {
	return atomic.LoadInt32(&s.status) != role.StatusRunning
}
