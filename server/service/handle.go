package service

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/golang/protobuf/ptypes/empty"
	pkgerr "github.com/pkg/errors"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/middleware"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
	"github.com/zywaited/delay-queue/role/timer"
	"github.com/zywaited/delay-queue/transport"
	"github.com/zywaited/go-common/xcopy"
)

type Handle struct {
	store    role.DataStore
	timer    timer.Scanner
	tp       task.Factory
	timeout  time.Duration
	runner   task.Runner
	baseTime time.Duration
	logger   system.Logger
	cp       *xcopy.XCopy
	ts       transport.TransporterM
}

func NewHandle(opts ...HandleOption) *Handle {
	h := &Handle{ts: make(transport.TransporterM)}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *Handle) generateId(addReq *pb.AddReq) string {
	bs, err := addReq.XXX_Marshal(nil, false)
	if err != nil {
		return ""
	}
	mh := md5.New()
	mh.Write(bs)
	return hex.EncodeToString(mh.Sum([]byte("med-delay-queue")))
}

func (h *Handle) add(uid string, addReq *pb.AddReq) (err error) {
	defer func() {
		if rerr := recover(); rerr != nil {
			if h.logger != nil {
				h.logger.Errorf("服务异常: %v, stack: %s", rerr, system.Stack())
			}
			err = errors.New("add task panic")
		}
	}()
	tt := task.RelativeTime
	if !addReq.Time.Relative {
		tt = task.AbsoluteTime
	}
	t := h.tp(
		task.ParamWithTime(task.Time{
			TTime: time.Duration(addReq.Time.Duration) * h.baseTime,
			TType: tt,
		}),
		task.ParamWithUid(uid),
		task.ParamWithName(addReq.Name),
		task.ParamWithRunner(h.runner),
		task.ParamWithType(pb.TaskType_TaskDelay),
	)
	now := time.Now().UnixNano()
	mt := model.GenerateTask()
	mt.Uid = uid
	mt.Name = addReq.Name
	mt.Type = int32(pb.TaskType_TaskDelay)
	mt.ExecTime = int64(t.Exec())
	mt.Schema = addReq.Callback.Schema
	mt.Address = addReq.Callback.Address
	mt.Path = addReq.Callback.Path
	mt.CreatedAt = now
	mt.UpdatedAt = now
	defer model.ReleaseTask(mt)
	err = pkgerr.WithMessage(h.store.Insert(mt), "任务写入存储失败")
	if err != nil {
		return
	}
	pt, ok := t.(task.Result)
	if ok {
		pt.InitResult()
	}
	err = pkgerr.WithMessage(h.timer.Add(t), "任务写入扫描器失败")
	if err != nil {
		return
	}
	if ok {
		err = pkgerr.WithMessage(pt.WaitResult(), "任务写入扫描器失败")
	}
	return
}

func (h *Handle) Add(ctx context.Context, addReq *pb.AddReq) (*pb.AddResp, error) {
	if _, err := govalidator.ValidateStruct(addReq); err != nil {
		return nil, xerror.WithXCode(xcode.RequestParamError)
	}
	if h.ts[transport.TransporterType(strings.ToUpper(strings.TrimSpace(addReq.Callback.Schema)))] == nil {
		return nil, xerror.WithXCodeMessagef(
			xcode.RequestParamError,
			"schema not support: %s",
			addReq.Callback.Schema,
		)
	}
	uid := h.generateId(addReq)
	if uid == "" {
		return nil, xerror.New("生成任务唯一标识失败")
	}
	traceId := ctx.Value(middleware.TraceIdKey)
	if h.logger != nil {
		h.logger.Infof("[%s]generate task uid: %s", traceId, uid)
	}
	c := make(chan error)
	go func() {
		c <- h.add(uid, addReq)
	}()
	select {
	case <-h.acTimeoutC():
		return nil, xerror.WithXCodeMessage(xcode.GatewayTimeout, "任务写入超时")
	case err := <-c:
		if err != nil {
			return nil, xerror.Wrap(err, "任务写入失败")
		}
		return &pb.AddResp{Uid: uid}, nil
	}
}

func (h *Handle) Get(ctx context.Context, req *pb.RetrieveReq) (*pb.Task, error) {
	if _, err := govalidator.ValidateStruct(req); err != nil {
		return nil, xerror.WithXCode(xcode.RequestParamError)
	}
	uid := strings.TrimSpace(req.Uid)
	c := make(chan error)
	var (
		mt  *model.Task
		err error
	)
	go func() {
		defer func() {
			if rerr := recover(); rerr != nil {
				if h.logger != nil {
					h.logger.Errorf("服务异常: %v, stack: %s", rerr, system.Stack())
				}
				c <- errors.New("retrieve task panic")
			}
		}()
		mt, err = h.store.Retrieve(uid)
		c <- pkgerr.WithMessage(err, "任务查询失败")
	}()
	select {
	case <-h.acTimeoutC():
		return nil, xerror.WithXCodeMessage(xcode.GatewayTimeout, "任务查询超时")
	case err = <-c:
		if err != nil {
			return nil, xerror.Wrap(err, "任务查询失败")
		}
		pt := &pb.Task{}
		if err = h.cp.SetSource(mt).CopyF(pt); err != nil {
			return nil, xerror.Wrap(err, "任务数据协议转换失败")
		}
		return pt, nil
	}
}

func (h *Handle) Remove(ctx context.Context, req *pb.RemoveReq) (*empty.Empty, error) {
	if _, err := govalidator.ValidateStruct(req); err != nil {
		return nil, xerror.WithXCode(xcode.RequestParamError)
	}
	uid := strings.TrimSpace(req.Uid)
	c := make(chan error)
	go func() {
		c <- h.remove(uid)
	}()
	select {
	case <-h.acTimeoutC():
		return nil, xerror.WithXCodeMessage(xcode.GatewayTimeout, "任务查询超时")
	case err := <-c:
		if err != nil {
			return nil, xerror.Wrap(err, "任务删除失败")
		}
		return &empty.Empty{}, nil
	}
}

func (h *Handle) acTimeoutC() (c <-chan time.Time) {
	if h.timeout > 0 {
		c = time.NewTimer(h.timeout * h.baseTime).C
	}
	return
}

func (h *Handle) remove(uid string) (err error) {
	defer func() {
		if rerr := recover(); rerr != nil {
			if h.logger != nil {
				h.logger.Errorf("服务异常: %v, stack: %s", rerr, system.Stack())
			}
			err = errors.New("remove task panic")
		}
	}()
	err = h.store.Remove(uid)
	if err != nil {
		err = pkgerr.WithMessage(err, "任务删除存储失败")
		return
	}
	t := h.tp(task.ParamWithUid(uid))
	defer t.Release()
	pt, ok := t.(task.Result)
	if ok {
		pt.InitResult()
	}
	err = pkgerr.WithMessage(h.timer.Remove(t), "任务删除扫描器失败")
	if err != nil {
		return
	}
	if ok {
		err = pkgerr.WithMessage(pt.WaitResult(), "任务删除扫描器失败")
	}
	return
}
