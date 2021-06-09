package server

import (
	"errors"
	"time"

	pkgerr "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/reload"
	"github.com/zywaited/delay-queue/role/runner"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/task/store"
	"github.com/zywaited/delay-queue/role/timer"
	"github.com/zywaited/delay-queue/role/timer/sorted"
	tw "github.com/zywaited/delay-queue/role/timer/timing-wheel"
)

type timerOption struct {
}

func NewTimerOption() *timerOption {
	return &timerOption{}
}

func (to *timerOption) Run(dq *DelayQueue) error {
	if dq.c.C.Role&uint(role.Timer) == 0 {
		return nil
	}
	sr := (timer.Scanner)(nil)
	switch dq.c.C.Timer.St {
	case string(tw.ServerName):
		sr = tw.NewServer(
			tw.NewServerConfig(tw.ServerConfigWithFactory(task.AcPoolFactory(task.DefaultTaskPoolFactory))),
			tw.OptionWithSlotNum(dq.c.C.Timer.TimingWheel.SlotNum),
			tw.OptionWithLogger(dq.c.CB.Logger),
			tw.OptionWithMaxLevel(dq.c.C.Timer.TimingWheel.MaxLevel),
			tw.OptionWithConfigScale(time.Duration(dq.c.C.ConfigScale)*dq.base),
			tw.OptionWithScaleLevel(time.Duration(dq.c.C.Timer.ConfigScaleLevel)*dq.base),
			tw.OptionWithNewTaskStore(store.Handler(store.DefaultStoreName)),
			tw.OptionWithGP(dq.gp),
		)
	case string(sorted.ServerName):
		sr = sorted.NewServer(
			sorted.ServerConfigWithLogger(dq.c.CB.Logger),
			sorted.ServerConfigWithScale(time.Duration(dq.c.C.ConfigScale/dq.c.C.Timer.ConfigScaleLevel)*dq.base),
			sorted.ServerConfigWithStore(store.Handler(store.SortedListStoreName)),
			sorted.ServerConfigWithGP(dq.gp),
		)
	}
	dq.timer = sr
	return pkgerr.WithMessage(sr.Run(), "扫描器启动失败")
}

func (to *timerOption) Stop(dq *DelayQueue) error {
	if dq.timer == nil {
		return nil
	}
	return pkgerr.WithMessage(dq.timer.Stop(role.GraceFulST), "扫描器暂停失败")
}

type runnerOption struct {
}

func NewRunnerOption() *runnerOption {
	return &runnerOption{}
}

func (ro *runnerOption) Run(dq *DelayQueue) error {
	rr := runner.NewRunner(
		dq.rq,
		dq.store,
		runner.OptionWithLogger(dq.c.CB.Logger),
		runner.OptionWithTimer(dq.timer),
		runner.OptionWithMaxCheckTime(dq.c.C.Timer.MaxCheckTime),
		runner.OptionWithCheckMulti(dq.c.C.Timer.CheckMulti),
		runner.OptionWithTaskPool(task.AcTaskPoolFactory(task.DefaultTaskPoolFactory)),
	)
	runner.RegisterRunner(runner.DefaultRunnerName, rr.Run)
	return nil
}

func (ro *runnerOption) Stop(_ *DelayQueue) error {
	return nil
}

type reloadOption struct {
}

func NewReloadOption() *reloadOption {
	return &reloadOption{}
}

func (ro *reloadOption) Run(dq *DelayQueue) error {
	if dq.c.C.Role&uint(role.Timer) == 0 {
		return nil
	}
	if dq.timer == nil || runner.AcRunner(runner.DefaultRunnerName) == nil {
		return errors.New("timer or runner not init")
	}
	if dq.c.C.Timer.St != string(tw.ServerName) {
		return nil
	}
	rs := reload.NewServer(
		reload.ServerConfigWithLogger(dq.c.CB.Logger),
		reload.ServerConfigWithReload(role.NewGeneratePool(dq.reloadStore, dq.convert)),
		reload.ServerConfigWithReloadGN(dq.c.C.Timer.TimingWheel.ReloadGoNum),
		reload.ServerConfigWithReloadScale(time.Duration(dq.c.C.Timer.TimingWheel.ReloadConfigScale)*dq.base),
		reload.ServerConfigWithReloadPerNum(dq.c.C.Timer.TimingWheel.ReloadPerNum),
		reload.ServerConfigWithTimer(dq.timer),
		reload.ServerConfigWithRunner(runner.AcRunner(runner.DefaultRunnerName)),
		reload.ServerConfigWithStore(dq.store),
		reload.ServerConfigWithGP(dq.gp),
		reload.ServerConfigWithGLS(dq.reloadStore),
	)
	return pkgerr.WithMessage(rs.Run(), "Reload启动失败")
}

func (ro *reloadOption) Stop(_ *DelayQueue) error {
	return nil
}
