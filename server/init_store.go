package server

import (
	"crypto/md5"
	"errors"

	"github.com/zywaited/delay-queue/inter"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/repository/mongo"
	"github.com/zywaited/delay-queue/repository/redis"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/runner"
	"github.com/zywaited/delay-queue/role/task/store"
	"github.com/zywaited/delay-queue/role/timer/sorted"
	tw "github.com/zywaited/delay-queue/role/timer/timing-wheel"
)

type storeOption struct {
	ids map[string]InitStoreFunc
	its map[string]InitStoreFunc
	irs map[string]InitStoreFunc
}

func NewStoreOption() *storeOption {
	so := &storeOption{}
	so.ids = map[string]InitStoreFunc{
		"":               so.initNotValidDataSource,
		inter.RedisStore: so.initRedisDataSource,
		inter.MongoStore: so.initMongoDataSource,
	}
	so.its = map[string]InitStoreFunc{
		"":                        so.initEmptyTimerStore,
		string(tw.ServerName):     so.initEmptyTimerStore,
		string(sorted.ServerName): so.initRedisTimerStore,
	}
	so.irs = map[string]InitStoreFunc{
		"":                so.initMemoryReadyQueue,
		inter.MemoryStore: so.initMemoryReadyQueue,
		inter.RedisStore:  so.initRedisReadyQueue,
	}
	return so
}

func (so *storeOption) initRedisDataSource(dq *DelayQueue) error {
	if dq.c.CB.Redis == nil {
		return errors.New("redis未初始化")
	}
	tws := redis.NewTWStore(
		dq.c.CB.Redis,
		redis.ConfigWithPrefix(dq.c.C.DataSource.Redis.Prefix),
		redis.ConfigWithName(dq.c.C.DataSource.Redis.Name),
		redis.ConfigWithId(dq.timerId),
		redis.ConfigWithConvert(dq.convert),
		redis.ConfigWithCopy(dq.cp),
	)
	dq.store = tws
	dq.reloadStore = redis.NewGenerateLoseStore(tws)
	return nil
}

func (so *storeOption) initMongoDataSource(dq *DelayQueue) error {
	if dq.c.CB.Mongo.Client == nil {
		return errors.New("mongo未初始化")
	}
	// 对timerId做MD5
	m := md5.New()
	m.Write([]byte(dq.timerId))
	token := string(m.Sum(nil))
	opts := []mongo.ConfigOption{
		mongo.ConfigWithCopy(dq.cp),
		mongo.ConfigWithConvert(dq.convert),
		mongo.ConfigWithToken(token),
		mongo.ConfigWithDb(dq.c.C.Mongo.DbName),
		mongo.ConfigWithCollection((&model.MongoTask{}).Collection()),
	}
	dq.reloadStore = mongo.NewGenerateLoseStore(dq.c.CB.Mongo.Client, opts...)
	if dq.c.CB.Mongo.Version < 4 || !dq.c.C.Mongo.Transaction {
		// note: 全局都是修改操作，这里用bulk write也不会有问题
		dq.store = mongo.NewLowerTransactionStore(dq.c.CB.Mongo.Client, opts...)
		return nil
	}
	dq.store = mongo.NewTransactionStore(dq.c.CB.Mongo.Client, opts...)
	return nil
}

func (so *storeOption) initNotValidDataSource(_ *DelayQueue) error {
	return errors.New("未知的存储类型")
}

func (so *storeOption) initRedisTimerStore(dq *DelayQueue) error {
	if dq.c.CB.Redis == nil {
		return errors.New("redis未初始化")
	}
	// 需要先注入存储
	store.RegisterHandler(
		store.SortedListStoreName,
		store.NewTaskPoolStore(
			store.NewSortedList(
				redis.NewSortedSet(
					dq.c.CB.Redis,
					dq.store,
					redis.ConfigWithPrefix(dq.c.C.DataSource.Redis.Prefix),
					redis.ConfigWithName(dq.c.C.DataSource.Redis.Name),
					redis.ConfigWithId(dq.timerId),
					redis.ConfigWithConvert(dq.convert),
					redis.ConfigWithCopy(dq.cp),
				),
			).NewSortedList,
			store.NewSortedListStoreIterator,
		).NewTaskStore,
	)
	return nil
}

func (so *storeOption) initEmptyTimerStore(_ *DelayQueue) error {
	return nil
}

func (so *storeOption) initRedisReadyQueue(dq *DelayQueue) error {
	if dq.c.CB.Redis == nil {
		return errors.New("redis未初始化")
	}
	dq.rq = redis.NewReadyQueue(
		dq.c.CB.Redis,
		dq.store,
		redis.ConfigWithPrefix(dq.c.C.DataSource.Redis.Prefix),
		redis.ConfigWithName(dq.c.C.DataSource.Redis.Name),
		redis.ConfigWithId(dq.workerId),
		redis.ConfigWithConvert(dq.convert),
		redis.ConfigWithCopy(dq.cp),
	)
	return nil
}

func (so *storeOption) initMemoryReadyQueue(dq *DelayQueue) error {
	dq.rq = runner.NewMemoryReadyQueue()
	return nil
}

func (so *storeOption) Run(dq *DelayQueue) error {
	err := so.ids[dq.c.C.DataSource.Dst](dq)
	if err != nil {
		return err
	}

	if dq.c.C.Role&uint(role.Timer) == 1 {
		err = so.its[dq.c.C.Timer.St](dq)
		if err != nil {
			return err
		}
	}

	// init ready-queue
	return so.irs[dq.c.C.DataSource.Rst](dq)
}

func (so *storeOption) Stop(_ *DelayQueue) error {
	return nil
}
