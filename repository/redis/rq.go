package redis

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
)

type readyQueue struct {
	c            *config
	rp           *redis.Pool
	store        role.DataStore
	absoluteName string // prefix + "_" + name
}

func NewReadyQueue(rp *redis.Pool, store role.DataStore, opts ...ConfigOption) *readyQueue {
	rq := &readyQueue{
		c:     NewConfig(opts...),
		rp:    rp,
		store: store,
	}
	rq.absoluteName = rq.c.prefix + "_" + rq.c.name
	return rq
}

func (rq *readyQueue) Len() int {
	c := rq.rp.Get().(redis.Conn)
	defer func() {
		_ = c.Close()
	}()
	l, _ := redis.Int(c.Do("LLEN", rq.absoluteName))
	return l
}

func (rq *readyQueue) Push(t task.Task) error {
	if t.Uid() == "" {
		return nil
	}
	c := rq.rp.Get().(redis.Conn)
	defer func() {
		_ = c.Close()
	}()
	_, err := c.Do("LPUSH", rq.absoluteName, t.Uid())
	return errors.WithMessage(err, "写入Redis队列失败")
}

func (rq *readyQueue) Pop() (task.Task, error) {
	c := rq.rp.Get().(redis.Conn)
	defer func() {
		_ = c.Close()
	}()
	// todo 后续可以改为BLPOP(暂时不用长连接是为了减少压力)
	uid, err := redis.String(c.Do("LPOP", rq.absoluteName))
	if err != nil {
		if err == redis.ErrNil {
			return nil, xerror.WithXCodeMessage(xcode.DBRecordNotFound, "任务不存在")
		}
		return nil, errors.WithMessage(err, "read-queue pop error")
	}
	// 这里就不查询详情了，只返回uid
	//mt, err := rq.store.Retrieve(uid)
	//if err != nil {
	//	return nil, errors.WithMessage(err, "read-queue store retrieve error")
	//}
	mt := model.GenerateTask()
	mt.Uid = uid
	mt.ExecTime = time.Now().UnixNano()
	defer model.ReleaseTask(mt)
	return rq.c.convert.Convert(mt), nil
}
