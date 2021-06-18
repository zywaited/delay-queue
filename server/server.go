package server

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	pkgerr "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/inter"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/runner"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
	"github.com/zywaited/delay-queue/transport"
	"github.com/zywaited/go-common/limiter"
	"github.com/zywaited/go-common/xcopy"
)

type DelayQueue struct {
	c           *inter.ConfigData
	store       role.DataStore
	reloadStore role.GenerateLoseStore
	base        time.Duration
	cp          xcopy.XCopy

	timer      timer.Scanner
	convert    role.PbConvertTask
	rq         runner.ReadyQueue
	worker     role.Launcher
	workerId   string
	timerId    string
	transports transport.TransporterM
	opts       []InitOption
	gp         limiter.Pool
	idCreator  role.GenerateIds

	exit func()
	ec   chan struct{}
}

func NewDelayQueue(opts ...InitOption) *DelayQueue {
	return &DelayQueue{cp: xcopy.NewCopy(), opts: opts, ec: make(chan struct{}, 1)}
}

func (dq *DelayQueue) Run() error {
	dq.exit = func() {
		select {
		case dq.ec <- struct{}{}:
		default:
		}
	}
	dq.convert = role.NewDefaultPbConvertTask(task.AcTaskPoolFactory(task.DefaultTaskPoolFactory))
	for _, opt := range dq.opts {
		err := opt.Run(dq)
		if err != nil {
			return pkgerr.WithMessage(err, "服务初始化失败")
		}
	}
	// loop
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	select {
	case <-dq.ec:
		if dq.c.CB.Logger != nil {
			dq.c.CB.Logger.Info("服务管道终止")
		}
	case sig := <-sc:
		if dq.c.CB.Logger != nil {
			dq.c.CB.Logger.Infof("服务信号终止: %s", sig)
		}
	}
	for _, opt := range dq.opts {
		err := opt.Stop(dq)
		if err != nil {
			return pkgerr.WithMessage(err, "服务停止失败")
		}
	}
	return nil
}

func (dq *DelayQueue) Stop(_ role.StopType) error {
	return nil
}
