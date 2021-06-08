package server

import (
	"time"

	pkgerr "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/worker"
	"github.com/zywaited/delay-queue/transport"
	trgrpc "github.com/zywaited/delay-queue/transport/grpc"
	trhttp "github.com/zywaited/delay-queue/transport/http"
)

type transportersOption struct {
}

func NewTransportersOption() *transportersOption {
	return &transportersOption{}
}

func (to *transportersOption) Run(dq *DelayQueue) error {
	timeout := time.Duration(0)
	if dq.c.C.Role&uint(role.Timer) != 0 {
		timeout = time.Duration(dq.c.C.Timer.Timeout)
	}
	if dq.c.C.Role&uint(role.Worker) != 0 {
		timeout = time.Duration(dq.c.C.Worker.Timeout)
	}
	timeout *= dq.base
	dq.transports = make(transport.TransporterM)
	th := trhttp.NewSender(trhttp.SenderOptionWithTimeout(timeout))
	dq.transports[transport.TransporterType(trhttp.SendersType)] = th
	dq.transports[transport.TransporterType(trhttp.SenderHttpsType)] = th
	dq.transports[transport.TransporterType(trgrpc.SendersType)] = trgrpc.NewSender(
		trgrpc.SenderOptionWithTimeout(timeout),
		trgrpc.SenderOptionWithLogger(dq.c.CB.Logger),
	)
	return nil
}

func (to *transportersOption) Stop(_ *DelayQueue) error {
	return nil
}

type workerOption struct {
}

func NewWorkerOption() *workerOption {
	return &workerOption{}
}

func (wo *workerOption) Run(dq *DelayQueue) error {
	if dq.c.C.Role&uint(role.Worker) == 0 {
		return nil
	}
	w := worker.NewScanner(
		worker.OptionWithLogger(dq.c.CB.Logger),
		worker.OptionsWithConfigScale(time.Duration(dq.c.C.ConfigScale)*dq.base),
		worker.OptionWithStore(dq.store),
		worker.OptionsWithReadyQueue(dq.rq),
		worker.OptionsWithRepeatedTimes(int(dq.c.C.Worker.RepeatedTimes)),
		worker.OptionsWithTimeout(time.Duration(dq.c.C.Worker.Timeout)*dq.base),
		worker.OptionsWithMultiNum(int(dq.c.C.Worker.MultiNum)),
		worker.OptionsWithRetryTimes(dq.c.C.Worker.RetryTimes),
		worker.OptionWithTransporters(dq.transports),
	)
	dq.worker = w
	return pkgerr.WithMessage(w.Run(), "Worker启动失败")
}

func (wo *workerOption) Stop(dq *DelayQueue) error {
	if dq.worker == nil {
		return nil
	}
	return pkgerr.WithMessage(dq.worker.Stop(role.GraceFulST), "Worker暂停失败")
}
