package redis

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/go-common/xcopy"
)

type Send func(commandName string, args ...interface{}) error

type mapStore struct {
	c  *config
	rp *redis.Pool

	absoluteName   string // prefix + "_" + name
	idAbsoluteName string // prefix + "_" + "_ID", 为了全局生成
	ap             *sync.Pool
}

func NewMapStore(rp *redis.Pool, opts ...ConfigOption) *mapStore {
	ms := &mapStore{
		c:  NewConfig(opts...),
		rp: rp,
		ap: &sync.Pool{New: func() interface{} {
			return make([]interface{}, 0, 20) // 先给20字段长度
		}},
	}
	ms.absoluteName = ms.c.storeName() + "_hash"
	ms.idAbsoluteName = ms.c.storeName() + "_id"
	if ms.c.cp == nil {
		ms.c.cp = xcopy.NewCopy()
	}
	return ms
}

func (ms *mapStore) Insert(t *model.Task) error {
	return ms.insert(t, ms.do)
}

func (ms *mapStore) Retrieve(uid string) (*model.Task, error) {
	c := ms.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	rt, err := redis.StringMap(c.Do("HGETALL", fmt.Sprintf("%s_%s", ms.absoluteName, uid)))
	if err != nil {
		if err == redis.ErrNil {
			return nil, xerror.WithXCodeMessage(xcode.DBRecordNotFound, "Redis任务不存在")
		}
		return nil, errors.WithMessage(err, "Redis数据获取失败")
	}
	if len(rt) == 0 {
		return nil, xerror.WithXCodeMessage(xcode.DBRecordNotFound, "Redis任务不存在")
	}
	t := model.GenerateTask()
	if err = ms.c.cp.CopySF(t, rt); err != nil {
		model.ReleaseTask(t)
		return nil, errors.WithMessage(err, "Redis数据协议转换失败[Retrieve]")
	}
	return t, nil
}

func (ms *mapStore) Remove(uid string) error {
	return ms.remove(uid, ms.do)
}

func (ms *mapStore) Batch(uids []string) ([]*model.Task, error) {
	c := ms.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	return ms.batch(c, uids)
}

func (ms *mapStore) InsertMany(ts []*model.Task) error {
	c := ms.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	err := ms.insertMany(ts, c.Send)
	if err != nil {
		return errors.WithMessage(err, "Redis数据批量写入失败")
	}
	return errors.WithMessage(c.Flush(), "Redis数据批量写入失败")
}

func (ms *mapStore) RemoveMany(uids []string) error {
	c := ms.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	err := ms.removeMany(uids, c.Send)
	if err != nil {
		return errors.WithMessage(err, "Redis数据批量删除失败")
	}
	return errors.WithMessage(c.Flush(), "Redis数据批量删除失败")
}

// todo 暂时不生成ID后面有需要再处理
func (ms *mapStore) generateIds(ts ...*model.Task) error {
	c := ms.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	id, err := redis.Int(c.Do("INCRBY", ms.idAbsoluteName, len(ts)))
	if err != nil {
		return errors.WithMessage(err, "Redis数据写入失败[ID]")
	}
	sId := id - len(ts) + 1
	for _, t := range ts {
		t.Id = strconv.Itoa(sId)
		sId++
	}
	return nil
}

func (ms *mapStore) insert(t *model.Task, send Send) error {
	args := ms.ap.Get().([]interface{})
	defer func() {
		args = args[:0]
		ms.ap.Put(args)
	}()
	args = append(args, fmt.Sprintf("%s_%s", ms.absoluteName, t.Uid))
	t.ConvertRedisTask().Args(&args)
	return errors.WithMessage(send("HMSET", args...), "Redis数据写入失败[TASK]")
}

func (ms *mapStore) remove(uid string, send Send) error {
	return errors.WithMessage(
		send("DEL", fmt.Sprintf("%s_%s", ms.absoluteName, uid)),
		"Redis数据删除失败",
	)
}

func (ms *mapStore) removeMany(uids []string, send Send) error {
	for _, uid := range uids {
		if err := ms.remove(uid, send); err != nil {
			return errors.WithMessage(err, "Redis数据批量删除失败")
		}
	}
	return nil
}

func (ms *mapStore) insertMany(ts []*model.Task, send Send) error {
	for _, t := range ts {
		if err := ms.insert(t, send); err != nil {
			return errors.WithMessage(err, "Redis数据批量写入失败")
		}
	}
	return nil
}

// 统一封装方便处理
func (ms *mapStore) do(commandName string, args ...interface{}) error {
	c := ms.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	_, err := c.Do(commandName, args...)
	return err
}

func (ms *mapStore) doV(commandName string, args ...interface{}) (interface{}, error) {
	c := ms.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	return c.Do(commandName, args...)
}

func (ms *mapStore) batch(c redis.Conn, uids []string) ([]*model.Task, error) {
	return ms.batchWithEmpty(c, uids, nil)
}

func (ms *mapStore) batchWithEmpty(c redis.Conn, uids []string, emptyFn func(string)) ([]*model.Task, error) {
	for _, uid := range uids {
		err := c.Send("HGETALL", fmt.Sprintf("%s_%s", ms.absoluteName, uid))
		if err != nil {
			return nil, errors.WithMessage(err, "Redis数据获取失败")
		}
	}
	err := c.Flush()
	if err != nil {
		return nil, errors.WithMessage(err, "Redis数据获取失败")
	}
	ts := make([]*model.Task, 0, len(uids))
	cp := ms.c.cp
	for _, uid := range uids {
		rt, err := redis.StringMap(c.Receive())
		if err != nil {
			if err == redis.ErrNil {
				if emptyFn != nil {
					emptyFn(uid)
				}
				continue
			}
			return nil, errors.WithMessage(err, "Redis数据获取失败")
		}
		if len(rt) == 0 {
			if emptyFn != nil {
				emptyFn(uid)
			}
			continue
		}
		t := model.GenerateTask()
		ts = append(ts, t)
		if err = cp.CopyF(t, rt); err != nil {
			for _, t = range ts {
				model.ReleaseTask(t)
			}
			return nil, errors.WithMessage(err, "Redis数据协议转换失败[Batch]")
		}
	}
	return ts, nil
}
