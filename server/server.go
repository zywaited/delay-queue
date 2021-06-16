package server

import (
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
}

func NewDelayQueue(opts ...InitOption) *DelayQueue {
	return &DelayQueue{cp: xcopy.NewCopy(), opts: opts}
}

func (dq *DelayQueue) Run() error {
	dq.convert = role.NewDefaultPbConvertTask(task.AcTaskPoolFactory(task.DefaultTaskPoolFactory))
	for _, opt := range dq.opts {
		err := opt.Run(dq)
		if err != nil {
			return pkgerr.WithMessage(err, "服务初始化失败")
		}
	}
	// loop
	// todo 后续加入信号监听
	select {}
}

func (dq *DelayQueue) Stop(_ role.StopType) error {
	return nil
}
