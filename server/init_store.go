package server

import (
	"errors"

	"github.com/zywaited/delay-queue/inter"
	"github.com/zywaited/delay-queue/repository/redis"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task/store"
	"github.com/zywaited/delay-queue/role/timer/sorted"
	tw "github.com/zywaited/delay-queue/role/timer/timing-wheel"
)

type storeOption struct {
}

func NewStoreOption() *storeOption {
	return &storeOption{}
}

func (so *storeOption) Run(dq *DelayQueue) error {
	rcs := []redis.ConfigOption{
		redis.ConfigWithPrefix(dq.c.C.DataSource.Redis.Prefix),
		redis.ConfigWithName(dq.c.C.DataSource.Redis.Name),
		redis.ConfigWithId(dq.timerId),
		redis.ConfigWithConvert(dq.convert),
		redis.ConfigWithCopy(dq.cp),
	}
	switch dq.c.C.DataSource.Dst {
	case inter.RedisStore:
		dq.store = redis.NewTWStore(dq.c.CB.Redis, rcs...)
	default:
		return errors.New("未知的存储类型")
	}

	if dq.c.C.Role&uint(role.Timer) == 1 {
		switch dq.c.C.Timer.St {
		case string(tw.ServerName):
		case string(sorted.ServerName):
			// 需要先注入存储
			store.RegisterHandler(
				store.SortedListStoreName,
				store.NewTaskPoolStore(
					store.NewSortedList(redis.NewSortedSet(dq.c.CB.Redis, dq.store, rcs...)).NewSortedList,
					store.NewSortedListStoreIterator,
				).NewTaskStore,
			)
		}
	}

	// init ready-queue
	rcs[2] = redis.ConfigWithId(dq.workerId) // 重置ID
	dq.rq = redis.NewReadyQueue(dq.c.CB.Redis, dq.store, rcs...)
	return nil
}

func (so *storeOption) Stop(_ *DelayQueue) error {
	return nil
}
