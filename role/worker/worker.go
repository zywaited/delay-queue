package worker

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/limiter"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/transport"
)

type Scanner struct {
	op     *option
	ts     transport.TransporterM
	status int32

	pbReq          *sync.Pool
	pbCallbackInfo *sync.Pool
}

func NewScanner(opts ...Options) *Scanner {
	s := &Scanner{
		op: &option{ts: make(transport.TransporterM)},
		pbReq: &sync.Pool{New: func() interface{} {
			return &pb.CallBackReq{}
		}},
		pbCallbackInfo: &sync.Pool{New: func() interface{} {
			return &pb.CallbackInfo{}
		}},
		status: role.StatusInitialized,
	}
	for _, opt := range opts {
		opt(s.op)
	}
	s.ts = s.op.ts
	return s
}

func (sr *Scanner) Run() error {
	for {
		cv := atomic.LoadInt32(&sr.status)
		if cv == role.StatusRunning {
			return nil
		}
		if !atomic.CompareAndSwapInt32(&sr.status, cv, role.StatusRunning) {
			continue
		}
		if cv == role.StatusForceSTW || cv == role.StatusInitialized {
			go sr.run()
		}
		return nil
	}
}

func (sr *Scanner) Stop(_ role.StopType) error {
	atomic.StoreInt32(&sr.status, role.StatusForceSTW)
	return nil
}

func (sr *Scanner) run() {
	defer func() {
		err := recover()
		if err == nil || sr.op.logger == nil {
			return
		}
		sr.op.logger.Infof("worker stop panic: %v, stack: %s", err, system.Stack())
	}()
	if sr.op.logger != nil {
		sr.op.logger.Infof("worker start")
	}
	maxTimeout := 8
	currentTimeout := 0
	for atomic.LoadInt32(&sr.status) == role.StatusRunning {
		t, err := sr.op.rq.Pop()
		if err != nil {
			if !xerror.IsXCode(err, xcode.DBRecordNotFound) {
				if sr.op.logger != nil {
					sr.op.logger.Infof("worker run task error: %v", err)
				}
				if currentTimeout < maxTimeout {
					currentTimeout++
				}
			}
			time.Sleep(sr.op.configScale << currentTimeout)
			continue
		}
		currentTimeout = 0
		_ = sr.op.gp.Submit(context.Background(), sr.newTaskJob(t))
	}
	if sr.op.logger != nil {
		sr.op.logger.Infof("worker stop")
	}
}

func (sr *Scanner) newTaskJob(t task.Task) limiter.TaskJob {
	return func() {
		defer t.Release()
		mt, err := sr.op.store.Retrieve(t.Uid())
		if err != nil {
			if sr.op.logger != nil {
				sr.op.logger.Infof("worker run task[%s] error[Retrieve]: %v", t.Uid(), err)
			}
			return
		}
		defer model.ReleaseTask(mt)
		if pb.TaskType(mt.Type) == pb.TaskType_Ignore {
			if sr.op.logger != nil {
				sr.op.logger.Infof("worker run task[%s] error[type: %d]", t.Uid(), mt.Type)
			}
			return
		}
		if pb.TaskType(mt.Type) == pb.TaskType_TaskFinished || mt.Times > 0 {
			return
		}
		tr := sr.ts[transport.TransporterType(strings.ToUpper(strings.TrimSpace(mt.Schema)))]
		if tr == nil {
			sr.changeStatus(t, pb.TaskType_Ignore)
			if sr.op.logger != nil {
				sr.op.logger.Infof("worker run task[%s] error[schema: %s]", mt.Uid, mt.Schema)
			}
			return
		}
		err = sr.send(tr, mt)
		if err != nil {
			return
		}
		sr.incrTaskSendTimes(t)
	}
}

func (sr *Scanner) send(tr transport.Transporter, mt *model.Task) (err error) {
	// 意义不大了，因为runner会重置
	//sr.changeStatus(t, pb.TaskType_TaskReserved)
	retryTimes := 0
	req := sr.pbReq.Get().(*pb.CallBackReq)
	req.Uid = mt.Uid
	req.Name = mt.Name
	req.Args = mt.Args
	req.Url = ""
	cd := sr.pbCallbackInfo.Get().(*pb.CallbackInfo)
	cd.Schema = mt.Schema
	cd.Address = mt.Address
	cd.Path = mt.Path
	defer func() {
		sr.pbReq.Put(req)
		sr.pbCallbackInfo.Put(cd)
	}()
	for retryTimes < sr.op.retryTimes {
		if retryTimes > 0 {
			// note: retry
			time.Sleep(sr.op.configScale << retryTimes)
		}
		// todo 后续判断是否可发送(重复发送)
		err = tr.Send(cd, req)
		if err != nil {
			retryTimes++
			continue
		}
		return
	}
	if sr.op.logger != nil {
		sr.op.logger.Infof("worker run task[%s] error: %v", mt.Uid, err)
	}
	return
}

func (sr *Scanner) incrTaskSendTimes(t task.Task) {
	if sr.op.store == nil {
		return
	}
	us, ok := sr.op.store.(role.DataStoreUpdater)
	if !ok {
		return
	}
	var err error
	fn := func(st role.DataStore) error {
		us = st.(role.DataStoreUpdater)
		err = us.IncrSendTimes(t.Uid(), 1)
		if err != nil {
			return err
		}
		return us.Status(t.Uid(), pb.TaskType_TaskFinished)
	}
	ts, ok := sr.op.store.(role.DataSourceTransaction)
	if ok {
		err = ts.Transaction(fn)
	} else {
		err = fn(sr.op.store)
	}
	if err == nil || sr.op.logger == nil {
		return
	}
	sr.op.logger.Infof("worker task[%s] success status error: %v", t.Uid(), err)
}

func (sr *Scanner) changeStatus(t task.Task, tt pb.TaskType) {
	if sr.op.store == nil {
		return
	}
	us, ok := sr.op.store.(role.DataStoreUpdater)
	if !ok {
		return
	}
	err := us.Status(t.Uid(), tt)
	if err == nil || sr.op.logger != nil {
		return
	}
	sr.op.logger.Infof("worker task[%s] reserved status error: %v", t.Uid(), err)
}
