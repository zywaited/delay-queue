package server

import (
	"errors"
	"time"

	"github.com/zywaited/delay-queue/role"
)

type currentIdOption struct {
}

func NewCurrentIdOption() *currentIdOption {
	return &currentIdOption{}
}

func (io *currentIdOption) Run(dq *DelayQueue) error {
	c := (<-chan time.Time)(nil)
	if dq.c.C.GenerateId.Timeout > 0 {
		c = time.NewTimer(time.Duration(dq.c.C.GenerateId.Timeout) * dq.base).C
	}
	d := make(chan struct{})
	go func() {
		dq.timerId, dq.workerId = role.AcGenerateId()()
		d <- struct{}{}
	}()
	select {
	case <-d:
	case <-c:
		return errors.New("generate id timeout")
	}
	if dq.c.CB.Logger != nil {
		dq.c.CB.Logger.Infof("SERVER NAME: %s: %s", dq.timerId, dq.workerId)
	}
	return nil
}

func (io *currentIdOption) Stop(_ *DelayQueue) error {
	return nil
}
