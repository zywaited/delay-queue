package server

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/asaskevich/govalidator"
	pkgerr "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/id"
)

const (
	defaultIdVersion = "default.name"
	localIdVersion   = "local.name"
	redisIdVersion   = "redis.name"
)

const (
	timerIDEnvName  = "MED_DELAY_QUEUE_TIMER_NAME"
	workerIDEnvName = "MED_DELAY_QUEUE_WORKER_NAME"
	nodeIDEnvName   = "MED_DELAY_QUEUE_NODE_NAME"
)

type currentIdOption struct {
}

func NewCurrentIdOption() *currentIdOption {
	return &currentIdOption{}
}

func (io *currentIdOption) localIdVersion(dq *DelayQueue) (role.GenerateIds, error) {
	workerId := os.Getenv(workerIDEnvName)
	timerId, err := os.Hostname()
	if err != nil {
		timerId = os.Getenv(timerIDEnvName)
	}
	if timerId == "" {
		return nil, errors.New("timer id empty")
	}
	nodeId, err := strconv.ParseInt(os.Getenv(nodeIDEnvName), 10, 64)
	if err != nil || nodeId == 0 {
		return nil, errors.New("node id empty")
	}
	return id.NewNodeId(
		id.OptionsWithLogger(dq.c.CB.Logger),
		id.OptionsWithNodeId(nodeId),
		id.OptionsWithTimerId(timerId),
		id.OptionsWithWorkerId(workerId),
	), nil
}

func (io *currentIdOption) configIdVersion(dq *DelayQueue) (role.GenerateIds, error) {
	_, err := govalidator.ValidateStruct(dq.c.C.GenerateId.Group)
	if err != nil {
		return nil, pkgerr.WithMessage(err, "generate-id's group config is error")
	}
	timerId := dq.c.C.GenerateId.Group.Group + "-" + dq.c.C.GenerateId.Group.Id
	return id.NewNodeId(
		id.OptionsWithLogger(dq.c.CB.Logger),
		id.OptionsWithNodeId(int64(dq.c.C.GenerateId.Group.Num)),
		id.OptionsWithTimerId(timerId),
	), nil
}

func (io *currentIdOption) redisIdVersion(dq *DelayQueue) (role.GenerateIds, error) {
	if dq.c.CB.Redis == nil {
		return nil, errors.New("redis未初始化")
	}
	opts := []id.Options{
		id.OptionsWithLogger(dq.c.CB.Logger),
		id.OptionsWithExitFunc(dq.exit),
	}
	if dq.c.C.GenerateId.Redis != nil && dq.c.C.GenerateId.Redis.Prefix != "" {
		opts = append(opts, id.OptionsWithPrefix(dq.c.C.GenerateId.Redis.Prefix))
	}
	if dq.c.C.GenerateId.Redis != nil && dq.c.C.GenerateId.Redis.HashKey != "" {
		opts = append(opts, id.OptionsWithHashKey(dq.c.C.GenerateId.Redis.HashKey))
	}
	if dq.c.C.GenerateId.Redis != nil && dq.c.C.GenerateId.Redis.CacheNum > 0 {
		opts = append(opts, id.OptionsWithCacheNum(int64(dq.c.C.GenerateId.Redis.CacheNum)))
	}
	if dq.c.C.GenerateId.Redis != nil && dq.c.C.GenerateId.Redis.CheckTime > 0 {
		opts = append(opts, id.OptionsWithCheckTime(dq.c.C.GenerateId.Redis.CheckTime))
	}
	if dq.c.C.GenerateId.Redis != nil && dq.c.C.GenerateId.Redis.ValidTime > 0 {
		opts = append(opts, id.OptionsWithValidTime(dq.c.C.GenerateId.Redis.ValidTime))
	}
	return id.NewRedisId(dq.c.CB.Redis, opts...), nil
}

func (io *currentIdOption) parseCreator(dq *DelayQueue) (role.GenerateIds, error) {
	if dq.c.C.GenerateId == nil {
		return io.localIdVersion(dq)
	}
	switch dq.c.C.GenerateId.Type {
	case localIdVersion:
		return io.localIdVersion(dq)
	case redisIdVersion:
		return io.redisIdVersion(dq)
	case defaultIdVersion:
		return io.configIdVersion(dq)
	default:
		version, err := io.localIdVersion(dq)
		if err != nil {
			version, err = io.configIdVersion(dq)
		}
		if err != nil {
			return nil, errors.New("invalid generate id type")
		}
		return version, nil
	}
}

func (io *currentIdOption) Run(dq *DelayQueue) error {
	version, err := io.parseCreator(dq)
	if err != nil {
		return pkgerr.WithMessage(err, "id 生成器初始化失败")
	}
	ctx := context.Background()
	if dq.c.C.GenerateId != nil && dq.c.C.GenerateId.Timeout > 0 {
		ctx, _ = context.WithTimeout(ctx, time.Duration(dq.c.C.GenerateId.Timeout)*dq.base)
	}
	ec := make(chan error, 1)
	go func() {
		verr := version.Run()
		dq.timerId, _ = version.Timer(ctx)
		dq.workerId, _ = version.Timer(ctx)
		ec <- verr
	}()
	select {
	case err = <-ec:
	case <-ctx.Done():
		return errors.New("generate id timeout")
	}
	if err != nil {
		return pkgerr.WithMessage(err, "id 生成器初始化失败")
	}
	dq.idCreator = version
	if dq.c.CB.Logger != nil {
		dq.c.CB.Logger.Infof("SERVER NAME: %s: %s", dq.timerId, dq.workerId)
	}
	return nil
}

func (io *currentIdOption) Stop(dq *DelayQueue) error {
	if dq.idCreator != nil {
		return dq.idCreator.Stop(role.ForceSTW)
	}
	return nil
}
