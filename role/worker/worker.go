package worker

import (
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
			go sr.tick()
		}
		return nil
	}
}

func (sr *Scanner) Stop(st role.StopType) error {
	switch st {
	case role.GraceFulST:
		atomic.CompareAndSwapInt32(&sr.status, role.StatusRunning, role.StatusGraceFulST)
	case role.ForceSTW:
		for {
			cv := atomic.LoadInt32(&sr.status)
			if cv == role.StatusForceSTW {
				return nil
			}
			atomic.CompareAndSwapInt32(&sr.status, cv, role.StatusForceSTW)
		}
	}
	return nil
}

func (sr *Scanner) tick() {
	if sr.op.logger != nil {
		sr.op.logger.Infof("worker start: %d", sr.op.multiNum)
	}
	c := make(chan struct{})
	defer close(c)
	closed := 0
	for num := 1; num <= sr.op.multiNum; num++ {
		go sr.run(c)
	}
	for range c {
		if atomic.LoadInt32(&sr.status) == role.StatusRunning {
			go sr.run(c)
			continue
		}
		closed++
		if closed == sr.op.multiNum {
			return
		}
	}
}

func (sr *Scanner) run(c chan struct{}) {
	defer func() {
		rerr := recover()
		c <- struct{}{} // 退出
		if rerr == nil || sr.op.logger == nil {
			return
		}
		sr.op.logger.Infof("worker run stop: %v, stack: %s", rerr, system.Stack())
	}()
	maxTimeout := 8
	currentTimeout := 0
	for {
		if atomic.LoadInt32(&sr.status) != role.StatusRunning {
			// make a chance to stop
			break
		}
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
		go sr.runTask(t)
		time.Sleep(sr.op.configScale)
		currentTimeout = 0
	}
}

func (sr *Scanner) runTask(t task.Task) {
	defer func() {
		rerr := recover()
		t.Release()
		if rerr == nil || sr.op.logger == nil {
			return
		}
		sr.op.logger.Infof("worker run task[%s] error: %v, stack: %s", t.Uid(), rerr, system.Stack())
	}()
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
		_ = sr.op.store.Remove(mt.Uid)
		return
	}
	if mt.Times > 0 {
		return
	}
	tr := sr.ts[transport.TransporterType(strings.ToUpper(strings.TrimSpace(mt.Schema)))]
	if tr == nil {
		sr.changeStatus(t, pb.TaskType_Ignore)
		if sr.op.logger != nil {
			sr.op.logger.Infof("worker run task[%s] error[schema: %s]", t.Uid(), mt.Schema)
		}
		return
	}
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
	for {
		// todo 后续判断是否可发送(重复发送)
		err = tr.Send(cd, req)
		if err != nil {
			if retryTimes >= sr.op.retryTimes {
				if sr.op.logger != nil {
					sr.op.logger.Infof("worker run task[%s] error: %v", t.Uid(), err)
				}
				break
			}
			retryTimes++
			time.Sleep(sr.op.configScale << retryTimes)
			continue
		}
		sr.incrTaskSendTimes(t)
		break
	}
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
