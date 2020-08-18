package redis

import (
	"fmt"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role"
)

// 为了方便重载数据单独提供一个存储
type TWStore struct {
	*MapStore

	absoluteName string // // prefix + "_" + name + "_set"

	stp *sync.Pool
}

func NewTWStore(rp *redis.Pool, opts ...ConfigOption) *TWStore {
	tws := &TWStore{
		MapStore: NewMapStore(rp, opts...),
		stp: &sync.Pool{New: func() interface{} {
			return &TWStoreTrans{}
		}},
	}
	tws.absoluteName = tws.c.absoluteName() + "_set"
	return tws
}

func (tws *TWStore) Transaction(fn func(role.DataStore) error) (err error) {
	c := tws.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	// pipe + multi
	err = c.Send("MULTI")
	if err != nil {
		return errors.WithMessage(err, "redis transaction")
	}
	defer func() {
		if re := recover(); re != nil {
			err = fmt.Errorf("redis transaction: %v, stack: %s", re, system.Stack())
		}
		if err == nil {
			err = errors.WithMessage(c.Send("EXEC"), "redis transaction ")
			if err == nil {
				// 自行处理flush，close也会处理，不会自行处理可以保留并且知道错误
				err = errors.WithMessage(c.Flush(), "redis transaction")
			}
		}
		if err != nil {
			_ = c.Send("DISCARD")
		}
	}()
	st := tws.stp.Get().(*TWStoreTrans)
	st.c = c
	st.TWStore = tws
	defer func() {
		st.c = nil
		st.TWStore = nil
		tws.stp.Put(st)
	}()
	err = errors.WithMessage(fn(st), "redis业务逻辑出错")
	return
}

// 重置添加和删除逻辑
func (tws *TWStore) Insert(t *model.Task) error {
	return errors.WithMessagef(tws.Transaction(func(st role.DataStore) error {
		return tws.interInsert(st.(*TWStoreTrans).c, t)
	}), "redis写入数据失败: %s", t.Uid)
}

func (tws *TWStore) interInsert(c redis.Conn, t *model.Task) error {
	err := tws.insert(t, c.Send)
	if err != nil {
		return err
	}
	// 把创建时间写入
	err = c.Send("ZADD", tws.absoluteName, t.CreatedAt, t.Uid)
	return errors.WithMessage(err, "redis zset 写入失败")
}

func (tws *TWStore) InsertMany(ts []*model.Task) error {
	return errors.WithMessage(tws.Transaction(func(st role.DataStore) error {
		return tws.interInsertMany(st.(*TWStoreTrans).c, ts)
	}), "redis批量写入数据失败")
}

func (tws *TWStore) interInsertMany(c redis.Conn, ts []*model.Task) error {
	err := tws.insertMany(ts, c.Send)
	if err != nil {
		return err
	}
	for _, t := range ts {
		err = c.Send("ZADD", tws.absoluteName, t.CreatedAt, t.Uid)
		if err != nil {
			return errors.WithMessage(err, "redis zset 批量写入失败")
		}
	}
	return nil
}

func (tws *TWStore) Remove(uid string) error {
	return errors.WithMessagef(tws.Transaction(func(st role.DataStore) error {
		return tws.interRemove(st.(*TWStoreTrans).c, uid)
	}), "redis删除数据失败", uid)
}

func (tws *TWStore) interRemove(c redis.Conn, uid string) error {
	err := tws.remove(uid, c.Send)
	if err != nil {
		return err
	}
	err = c.Send("ZREM", tws.absoluteName, uid)
	return errors.WithMessage(err, "redis set 删除失败")
}

func (tws *TWStore) RemoveMany(uids []string) error {
	return errors.WithMessage(tws.Transaction(func(st role.DataStore) error {
		return tws.interRemoveMany(st.(*TWStoreTrans).c, uids)
	}), "redis批量删除失败")
}

func (tws *TWStore) interRemoveMany(c redis.Conn, uids []string) error {
	err := tws.removeMany(uids, c.Send)
	if err != nil {
		return err
	}
	for _, uid := range uids {
		err = c.Send("ZREM", tws.absoluteName, uid)
		if err != nil {
			return errors.WithMessage(err, "redis zset 批量删除失败")
		}
	}
	return nil
}

func (tws *TWStore) rangeReady(st, et, offset, limit int64) ([]*model.Task, error) {
	c := tws.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	uids, err := redis.Strings(c.Do("ZRANGEBYSCORE", tws.absoluteName, st, et, "LIMIT", offset, limit))
	if err != nil {
		// nil
		if err == redis.ErrNil {
			return make([]*model.Task, 0), nil
		}
		return nil, errors.WithMessage(err, "redis查询当前分值的任务失败")
	}
	return tws.batch(c, uids)
}

func (tws *TWStore) readyNum(st, et int64) (int, error) {
	c := tws.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	l, err := redis.Int(c.Do("ZCOUNT", tws.absoluteName, st, et))
	if err != nil {
		if err == redis.ErrNil {
			return l, nil
		}
		return l, errors.WithMessage(err, "redis查询当前分值的数量")
	}
	return l, nil
}

func (tws *TWStore) Status(uid string, tt pb.TaskType) error {
	// redis数据直接删除即可
	if tt == pb.TaskType_TaskFinished || tt == pb.TaskType_Ignore {
		return tws.Remove(uid)
	}
	return tws.MapStore.Status(uid, tt)
}
