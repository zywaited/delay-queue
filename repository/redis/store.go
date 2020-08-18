package redis

import (
	"fmt"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/go-common/xcopy"
)

type Send func(commandName string, args ...interface{}) error

type MapStore struct {
	c  *config
	rp *redis.Pool

	absoluteName   string // prefix + "_" + name
	idAbsoluteName string // prefix + "_" + "_ID", 为了全局生成
	ap             *sync.Pool
}

func NewMapStore(rp *redis.Pool, opts ...ConfigOption) *MapStore {
	ms := &MapStore{
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

func (ms *MapStore) Insert(t *model.Task) error {
	return ms.insert(t, ms.do)
}

func (ms *MapStore) Retrieve(uid string) (*model.Task, error) {
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
	if err = ms.c.cp.SetSource(rt).CopySF(t); err != nil {
		model.ReleaseTask(t)
		return nil, errors.WithMessage(err, "Redis数据协议转换失败[Retrieve]")
	}
	return t, nil
}

func (ms *MapStore) Remove(uid string) error {
	return ms.remove(uid, ms.do)
}

func (ms *MapStore) Batch(uids []string) ([]*model.Task, error) {
	c := ms.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	return ms.batch(c, uids)
}

func (ms *MapStore) InsertMany(ts []*model.Task) error {
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

func (ms *MapStore) RemoveMany(uids []string) error {
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
func (ms *MapStore) generateIds(ts ...*model.Task) error {
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
		t.Id = uint(sId)
		sId++
	}
	return nil
}

func (ms *MapStore) insert(t *model.Task, send Send) error {
	args := ms.ap.Get().([]interface{})
	defer func() {
		args = args[:0]
		ms.ap.Put(args)
	}()
	args = append(args, fmt.Sprintf("%s_%s", ms.absoluteName, t.Uid))
	t.ConvertRedisTask().Args(&args)
	return errors.WithMessage(send("HMSET", args...), "Redis数据写入失败[TASK]")
}

func (ms *MapStore) remove(uid string, send Send) error {
	return errors.WithMessage(
		send("DEL", fmt.Sprintf("%s_%s", ms.absoluteName, uid)),
		"Redis数据删除失败",
	)
}

func (ms *MapStore) removeMany(uids []string, send Send) error {
	for _, uid := range uids {
		if err := ms.remove(uid, send); err != nil {
			return errors.WithMessage(err, "Redis数据批量删除失败")
		}
	}
	return nil
}

func (ms *MapStore) insertMany(ts []*model.Task, send Send) error {
	for _, t := range ts {
		if err := ms.insert(t, send); err != nil {
			return errors.WithMessage(err, "Redis数据批量写入失败")
		}
	}
	return nil
}

func (ms *MapStore) Status(uid string, tt pb.TaskType) error {
	// redis数据直接删除即可
	if tt == pb.TaskType_TaskFinished || tt == pb.TaskType_Ignore {
		return ms.Remove(uid)
	}
	// 只有delay状态要变更
	if tt == pb.TaskType_TaskDelay {
		return ms.status(uid, tt, ms.do)
	}
	return nil
}

func (ms *MapStore) status(uid string, tt pb.TaskType, send Send) error {
	key := fmt.Sprintf("%s_%s", ms.absoluteName, uid)
	return errors.WithMessage(send("HSET", key, "type", int32(tt)), "Redis数据更新状态失败")
}

func (ms *MapStore) NextTime(uid string, nt *time.Time) error {
	return ms.nextTime(uid, nt, ms.do)
}

func (ms *MapStore) nextTime(uid string, nt *time.Time, send Send) error {
	key := fmt.Sprintf("%s_%s", ms.absoluteName, uid)
	return errors.WithMessage(send("HSET", key, "next_exec_time", nt.UnixNano()), "Redis数据更新下次执行时间失败")
}

func (ms *MapStore) IncrRetryTimes(uid string, num int) error {
	return ms.incrRetryTimes(uid, num, ms.do)
}

func (ms *MapStore) incrRetryTimes(uid string, num int, send Send) error {
	key := fmt.Sprintf("%s_%s", ms.absoluteName, uid)
	return errors.WithMessage(send("HINCRBY", key, "retry_times", num), "Redis数据更新重试次数失败")
}

func (ms *MapStore) IncrSendTimes(uid string, num int) error {
	return ms.incrSendTimes(uid, num, ms.do)
}

func (ms *MapStore) incrSendTimes(uid string, num int, send Send) error {
	key := fmt.Sprintf("%s_%s", ms.absoluteName, uid)
	return errors.WithMessage(send("HINCRBY", key, "times", num), "Redis数据更新发送次数失败")
}

// 统一封装方便处理
func (ms *MapStore) do(commandName string, args ...interface{}) error {
	c := ms.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	_, err := c.Do(commandName, args...)
	return err
}

func (ms *MapStore) batch(c redis.Conn, uids []string) ([]*model.Task, error) {
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
	for range uids {
		rt, err := redis.StringMap(c.Receive())
		if err != nil {
			if err == redis.ErrNil {
				continue
			}
			return nil, errors.WithMessage(err, "Redis数据获取失败")
		}
		if len(rt) == 0 {
			continue
		}
		xcopy.WithSource(rt)(cp)
		t := model.GenerateTask()
		ts = append(ts, t)
		if err = cp.CopyF(t); err != nil {
			for _, t = range ts {
				model.ReleaseTask(t)
			}
			return nil, errors.WithMessage(err, "Redis数据协议转换失败[Batch]")
		}
	}
	return ts, nil
}
