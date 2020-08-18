package task

import (
	"time"

	"github.com/zywaited/delay-queue/protocol/pb"
)

type TimeType int32

const (
	RelativeTime TimeType = iota

	AbsoluteTime
)

type (
	Time struct {
		TTime time.Duration

		TType TimeType
	}

	Timer interface {
		Created() time.Duration
		Exec() time.Duration
		Delay() time.Duration     // 间隔时间
		Remaining() time.Duration // 还需等待时间
		WaitReady()               // 一直等待时间结束
	}

	Task interface {
		Timer

		Name() string
		Uid() string
		Run() error
		Release()
		Type() pb.TaskType
	}

	RunnerTask interface {
		InitRunner(Runner)
		ACRunner() Runner
	}

	Result interface {
		InitResult()
		CloseResult(error)
		WaitResult() error
	}

	Runner func(Task) error
)

type task struct {
	*param

	delay   time.Duration
	exec    time.Duration
	created time.Duration
}

func NewTask(opts ...ParamField) *task {
	t := &task{param: NewParam()}
	return t.init(opts...)
}

func (t *task) init(opts ...ParamField) *task {
	for _, opt := range opts {
		opt(t.param)
	}
	now := time.Duration(time.Now().UnixNano())
	t.created = now
	t.exec = now
	switch t.param.time.TType {
	case AbsoluteTime:
		if t.param.time.TTime > now {
			t.exec = t.param.time.TTime
			t.delay = t.exec - now
		}
	case RelativeTime:
		fallthrough
	default:
		// 默认都是相对时间
		if t.param.time.TTime > 0 {
			t.exec = now + t.param.time.TTime
			t.delay = t.param.time.TTime
		}
	}
	return t
}

func (t *task) Created() time.Duration {
	return t.created
}

func (t *task) Exec() time.Duration {
	return t.exec
}

func (t *task) Delay() time.Duration {
	return t.delay
}

func (t *task) Remaining() time.Duration {
	return t.exec - time.Duration(time.Now().UnixNano())
}

func (t *task) WaitReady() {
	d := t.Remaining()
	if d <= 0 {
		return
	}
	tr := time.NewTimer(d)
	select {
	case <-tr.C:
	}
}

func (t *task) Release() {
}

func (t *task) Run() error {
	if t.ACRunner() == nil {
		return nil
	}
	r := t.ACRunner()
	ParamWithRunner(nil)(t.param)
	return r(t)
}

type taskResult struct {
	*task

	c      chan error
	closed bool
}

func NewTaskResult(opts ...ParamField) *taskResult {
	return &taskResult{
		task:   NewTask(opts...),
		closed: true,
	}
}

func (r *taskResult) InitResult() {
	// 这里需要缓冲，不然很多逻辑需要异步处理
	r.c = make(chan error, 1)
	r.closed = false
}

func (r *taskResult) CloseResult(err error) {
	if !r.closed {
		r.closed = true
		r.c <- err
		close(r.c)
	}
}

func (r *taskResult) WaitResult() error {
	if !r.closed && r.c != nil {
		return <-r.c
	}
	return nil
}

func (r *taskResult) Release() {
	r.task.Release()
	r.CloseResult(nil)
}

type runnerTask struct {
	*taskResult
}

func NewRunnerTask(opts ...ParamField) *runnerTask {
	return &runnerTask{NewTaskResult(opts...)}
}

func (rt *runnerTask) InitRunner(r Runner) {
	rt.task.init(ParamWithRunner(r))
}

type bindTask struct {
	*runnerTask

	p Pool
}

func NewBindTask(opts ...ParamField) *bindTask {
	return &bindTask{runnerTask: NewRunnerTask(opts...)}
}

func (bt *bindTask) Bind(p Pool) {
	bt.p = p
}

func (bt *bindTask) Release() {
	bt.runnerTask.Release()
	p := bt.p
	bt.p = nil
	p.ReleaseTask(bt)
}
