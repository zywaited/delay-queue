package timing_wheel

import (
	"encoding/binary"
	"time"

	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role/task"
)

type DecorateTask interface {
	Decorate(t task.Task) Task
}

type TaskProtoOption func(tp *taskProto)

func TaskProtoOptionWithTaskFactory(factory task.Factory) TaskProtoOption {
	return func(tp *taskProto) {
		tp.tp = factory
	}
}

func TaskProtoOptionWithPoolFactory(factory task.PoolFactory) TaskProtoOption {
	return func(tp *taskProto) {
		tp.pf = factory
	}
}

func TaskProtoOptionWithConfigScale(scale time.Duration) TaskProtoOption {
	return func(tp *taskProto) {
		tp.configScale = scale
	}
}
func TaskProtoOptionWithScaleLevel(scaleLevel time.Duration) TaskProtoOption {
	return func(tp *taskProto) {
		tp.configScaleLevel = scaleLevel
	}
}
func TaskProtoOptionWithRunnerFactor(factor func() task.Runner) TaskProtoOption {
	return func(tp *taskProto) {
		tp.runnerFactor = factor
	}
}

const defaultMaxNameLength = 46

type taskProto struct {
	p task.Pool

	tp           task.Factory
	pf           task.PoolFactory
	runnerFactor func() task.Runner
	// 时间轮的刻度
	scale            time.Duration
	configScale      time.Duration
	configScaleLevel time.Duration
}

func NewTaskProto(opts ...TaskProtoOption) *taskProto {
	tp := &taskProto{}
	for _, opt := range opts {
		opt(tp)
	}
	tp.p = tp.pf.NewPool(newBindJobTask)
	tp.scale = realScaleOption(tp.configScale, tp.configScaleLevel)
	return tp
}

func (tp *taskProto) Marshal(ot task.Task) ([]byte, error) {
	t := (ot).(Task)
	var bs []byte
	// uid
	bs = append(bs, byte(len(t.Uid())))
	bs = append(bs, t.Uid()...)
	// name
	if len(t.Name()) <= defaultMaxNameLength {
		bs = append(bs, byte(len(t.Name())))
		bs = append(bs, t.Name()...)
	} else {
		bs = append(bs, byte(defaultMaxNameLength))
		bs = append(bs, t.Name()[:defaultMaxNameLength-3]...)
		bs = append(bs, '.', '.', '.')
	}
	// exec time
	us := make([]byte, 8)
	binary.BigEndian.PutUint64(us, uint64(t.Exec()))
	bs = append(bs, us...)
	// circle
	bs = append(bs, byte(t.Circle()))
	// type
	bs = append(bs, byte(t.Type()))
	return bs, nil
}

func (tp *taskProto) UnMarshal(bs []byte) (task.Task, error) {
	// uid
	uid := string(bs[1 : int(bs[0])+1])
	bs = bs[int(bs[0])+1:]
	// name
	name := string(bs[1 : int(bs[0])+1])
	bs = bs[int(bs[0])+1:]
	// exec time
	exec := int64(binary.BigEndian.Uint64(bs))
	bs = bs[8:]
	// circle
	circle := int(bs[0])
	// type
	taskType := pb.TaskType(bs[1])

	ot := tp.tp(
		task.ParamWithTime(task.Time{
			TTime: time.Duration(exec),
			TType: task.AbsoluteTime,
		}),
		task.ParamWithUid(uid),
		task.ParamWithName(name),
		task.ParamWithRunner(tp.runnerFactor()),
		task.ParamWithType(taskType),
	)
	t := tp.Decorate(ot)
	// note: pos 不解析
	t.index(-1, circle)
	t.scale(tp.scale, tp.configScaleLevel)
	return t, nil
}

func (tp *taskProto) Decorate(t task.Task) Task {
	j := (Task)(nil)
	if tp.p == nil {
		// 池化不存在也可以执行
		j = NewBindJob()
	} else {
		j = tp.p.NewTask().(Task)
		if pj, ok := j.(task.PoolTask); ok {
			pj.Bind(tp.p)
		}
	}
	if jp, ok := j.(poolJob); ok {
		jp.Extend(t)
	}
	// 最小刻度&配置级别, 用于判断任务是否到期
	j.scale(tp.scale, tp.configScaleLevel)
	return j
}
