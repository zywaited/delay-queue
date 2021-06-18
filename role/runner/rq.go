package runner

import (
	"math"
	"time"

	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
)

type ReadyQueue interface {
	Push(task.Task) error
	Pop() (task.Task, error)
	Len() int
}

type (
	ReadyQueueT string

	ReadyQueueIF interface {
		Name() string
		Type() ReadyQueueT
	}
)

type Option func(*runner)

func OptionWithReadyQueue(rq ReadyQueue) Option {
	return func(r *runner) {
		r.rq = rq
	}
}

func OptionWithDataSource(sd role.DataStore) Option {
	return func(r *runner) {
		r.sd = sd
	}
}

func OptionWithLogger(logger system.Logger) Option {
	return func(r *runner) {
		r.logger = logger
	}
}

func OptionWithTimer(st timer.Scanner) Option {
	return func(r *runner) {
		r.st = st
	}
}

func OptionWithMaxCheckTime(maxCheckTime int) Option {
	return func(r *runner) {
		if maxCheckTime > 0 {
			r.maxCheckTime = maxCheckTime
		}
	}
}

func OptionWithCheckMulti(checkMulti int) Option {
	return func(r *runner) {
		if checkMulti > 1 {
			r.checkMulti = checkMulti
		}
	}
}

func OptionWithTaskPool(tp task.Factory) Option {
	return func(r *runner) {
		r.tp = tp
	}
}

type runner struct {
	rq ReadyQueue
	sd role.DataStore

	logger       system.Logger
	st           timer.Scanner
	maxCheckTime int
	checkMulti   int
	tp           task.Factory
}

func NewRunner(rq ReadyQueue, sd role.DataStore, opts ...Option) *runner {
	r := &runner{rq: rq, sd: sd, maxCheckTime: math.MaxInt32, checkMulti: 1}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *runner) Run(tk task.Task) error {
	// 不引入复杂性, 这里不返回错误
	// 只需要入队列成功即可
	mt, err := r.sd.Retrieve(tk.Uid())
	if err != nil {
		if !xerror.IsXCode(err, xcode.DBRecordNotFound) {
			r.retryDelay(tk)
			return nil
		}
		// todo 这里查询不到数据有可能是被其他worker处理或者是删除，这里再删除一次
		r.removeNotExistTask(tk)
		return nil
	}
	defer model.ReleaseTask(mt)
	// 删除脏数据
	if pb.TaskType(mt.Type) == pb.TaskType_Ignore {
		if r.logger != nil {
			r.logger.Infof("runner run task[%s] error[type: %d]", tk.Uid(), mt.Type)
		}
		_ = r.sd.Remove(mt.Uid)
		return nil
	}
	if pb.TaskType(mt.Type) == pb.TaskType_TaskFinished || mt.Times > 0 {
		if r.logger != nil {
			r.logger.Infof("runner run task[%s] finished", tk.Uid())
		}
		return nil
	}
	if mt.RetryTimes > r.maxCheckTime {
		// 已经多出一次用来检测
		r.changeStatus(tk, pb.TaskType_Ignore)
		r.retryCheck(mt) // 为了打印日志
		return nil
	}
	err = r.rq.Push(tk)
	if err == nil {
		r.incrTaskRetryTimes(tk)
		r.retryCheck(mt)
		return nil
	}
	r.retryDelay(tk)
	return nil
}

func (r *runner) retryCheck(mt *model.Task) {
	mt.RetryTimes++
	if mt.RetryTimes > r.maxCheckTime+1 {
		if r.logger != nil {
			r.logger.Infof("worker task[%s] retry max times", mt.Uid)
		}
		return
	}
	nt := r.tp(
		task.ParamWithTime(task.Time{
			TTime: mt.NextDelayTime(),
			TType: task.RelativeTime,
		}),
		task.ParamWithUid(mt.Uid),
		task.ParamWithRunner(r.Run),
		task.ParamWithType(pb.TaskType_TaskCheck),
	)
	r.add(nt)
}

func (r *runner) retryDelay(tk task.Task) {
	if r.logger != nil {
		r.logger.Infof("worker task[%s] delay: continue", tk.Uid())
	}
	nt := r.tp(
		task.ParamWithTime(task.Time{
			TTime: time.Second * time.Duration(r.checkMulti),
			TType: task.RelativeTime,
		}),
		task.ParamWithUid(tk.Uid()),
		task.ParamWithRunner(r.Run),
		task.ParamWithType(pb.TaskType_TaskDelay),
	)
	r.add(nt)
}

func (r *runner) add(nt task.Task) {
	maxTimes := (r.maxCheckTime >> 1) + 1
	currentTimes := 0
	for {
		err := r.st.Add(nt)
		if err == nil || currentTimes == maxTimes {
			if err != nil && r.logger != nil {
				r.logger.Infof("worker task[%s] retry add max times: %v", nt.Uid(), err)
			}
			break
		}
		currentTimes++
		time.Sleep((time.Second << currentTimes) * time.Duration(r.checkMulti))
	}
}

func (r *runner) incrTaskRetryTimes(tk task.Task) {
	us, ok := r.sd.(role.DataStoreUpdater)
	if !ok {
		return
	}
	var err error
	fn := func(st role.DataStore) error {
		us = st.(role.DataStoreUpdater)
		err = us.IncrRetryTimes(tk.Uid(), 1)
		if err != nil {
			return err
		}
		return us.Status(tk.Uid(), pb.TaskType_TaskRetryDelay)
	}
	ts, ok := r.sd.(role.DataSourceTransaction)
	if ok {
		err = ts.Transaction(fn)
	} else {
		err = fn(r.sd)
	}
	if err == nil || r.logger == nil {
		return
	}
	r.logger.Infof("worker task[%s] retry status error: %v", tk.Uid(), err)
}

func (r *runner) changeStatus(t task.Task, tt pb.TaskType) {
	if r.sd == nil {
		return
	}
	us, ok := r.sd.(role.DataStoreUpdater)
	if !ok {
		return
	}
	err := us.Status(t.Uid(), tt)
	if err == nil || r.logger == nil {
		return
	}
	r.logger.Infof("worker task[%s] status ready error: %v", t.Uid(), err)
}

func (r *runner) removeNotExistTask(t task.Task) {
	if r.sd == nil {
		return
	}
	err := r.sd.Remove(t.Uid())
	if err == nil || r.logger == nil {
		return
	}
	r.logger.Infof("worker task[%s] remove error: %v", t.Uid(), err)
}
