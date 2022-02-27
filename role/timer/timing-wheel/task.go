package timing_wheel

import (
	"math"
	"time"

	"github.com/zywaited/delay-queue/role/task"
)

type poolJob interface {
	// 继承
	Extend(task.Task)
}

type Task interface {
	task.Task

	Pos() int
	Circle() int
	Ready() bool

	/**
	 * @param scale time.Duration 真实刻度
	 * @param scaleLevel time.Duration 级别
	 */
	scale(time.Duration, time.Duration)
	index(int, int)
}

type job struct {
	task.Task
	task.Result // 内部有task.Task转换

	pos        int // 时间轮当前轮节点位置
	circle     int // 圈数
	scaleBase  time.Duration
	scaleLevel time.Duration
}

func newBindJobTask() task.Task {
	return NewBindJob()
}

func newJob(opts ...jobOption) *job {
	j := &job{}
	j.opts(opts...)
	return j
}

func (j *job) opts(opts ...jobOption) {
	for _, opt := range opts {
		opt(j)
	}
}

func (j *job) Remaining() time.Duration {
	return time.Duration(math.Ceil(float64(j.Task.Remaining()) / float64(j.scaleLevel)))
}

func (j *job) index(pos, circle int) {
	j.opts(jobOptionWithPos(pos), jobOptionWithCircle(circle))
}

func (j *job) Pos() int {
	return j.pos
}

func (j *job) Circle() int {
	return j.circle
}

func (j *job) scale(scale, level time.Duration) {
	j.opts(jobOptionWithScale(scale), jobOptionWithScaleLevel(level))
}

func (j *job) Ready() bool {
	if j.circle > 0 {
		j.circle--
	}
	// 这里就直接以系统时间为准
	return j.circle < 1 && j.Remaining() < j.scaleBase
}

func (j *job) Release() {
	t := j.Task
	j.Task = nil
	r := j.Result
	j.Result = nil
	r.CloseResult(nil)
	t.Release()
}

type bindJob struct {
	*job

	p task.Pool
}

func NewBindJob(opts ...jobOption) *bindJob {
	return &bindJob{job: newJob(opts...)}
}

func (bj *bindJob) Bind(p task.Pool) {
	bj.p = p
}

func (bj *bindJob) Extend(t task.Task) {
	bj.Task = t
	if pt, ok := t.(task.Result); ok {
		bj.Result = pt
	}
}

func (bj *bindJob) Release() {
	bj.job.Release()
	p := bj.p
	bj.p = nil
	p.ReleaseTask(bj)
}
