package redis

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role"
)

const (
	ReloadEndStatus = iota
	ReloadPendingStatus
)

type ReloadTWStore struct {
	*TWStore

	reloadStatus int32  // 是否重载完成
	absoluteName string // prefix + "_" + name + "_remove_id_list"
	rstp         *sync.Pool
}

func NewReloadTWStore(tws *TWStore) *ReloadTWStore {
	rtws := &ReloadTWStore{
		TWStore:      tws,
		reloadStatus: ReloadEndStatus,
		rstp: &sync.Pool{New: func() interface{} {
			return &ReloadTWStoreTrans{}
		}},
	}
	rtws.absoluteName = tws.c.absoluteName() + "_remove_id_list"
	return rtws
}

func (rtws *ReloadTWStore) Start() {
	atomic.StoreInt32(&rtws.reloadStatus, ReloadPendingStatus)
}

func (rtws *ReloadTWStore) End() {
	atomic.StoreInt32(&rtws.reloadStatus, ReloadEndStatus)
}

// 删除功能延迟处理
func (rtws *ReloadTWStore) Remove(uid string) error {
	if atomic.LoadInt32(&rtws.reloadStatus) == ReloadEndStatus {
		return rtws.TWStore.Remove(uid)
	}
	return errors.WithMessage(rtws.do("LPUSH", rtws.absoluteName, uid), "写入删除队列失败")
}

func (rtws *ReloadTWStore) RemoveMany(uids []string) error {
	if atomic.LoadInt32(&rtws.reloadStatus) == ReloadEndStatus {
		return rtws.TWStore.RemoveMany(uids)
	}
	return rtws.Transaction(func(s role.DataStore) error {
		return rtws.interRemoveMany(s.(*TWStoreTrans).c, uids)
	})
}

func (rtws *ReloadTWStore) Status(uid string, tt pb.TaskType) error {
	// redis数据直接删除即可
	if tt == pb.TaskType_TaskFinished || tt == pb.TaskType_Ignore {
		return rtws.Remove(uid)
	}
	return rtws.TWStore.Status(uid, tt)
}

func (rtws *ReloadTWStore) Pop() (string, error) {
	uid, err := redis.String(rtws.doV("LPOP", rtws.absoluteName))
	if err != nil {
		if err == redis.ErrNil {
			return "", xerror.WithXCodeMessage(xcode.DBRecordNotFound, "任务不存在")
		}
		return "", errors.WithMessage(err, "remove uid queue pop")
	}
	return uid, err
}

func (rtws *ReloadTWStore) Len() int {
	l, _ := redis.Int(rtws.doV("LLEN", rtws.absoluteName))
	return l
}

func (rtws *ReloadTWStore) Transaction(fn func(role.DataStore) error) (err error) {
	c := rtws.rp.Get()
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
	st := rtws.rstp.Get().(*ReloadTWStoreTrans)
	st.c = c
	st.ReloadTWStore = rtws
	defer func() {
		st.c = nil
		st.ReloadTWStore = nil
		rtws.rstp.Put(st)
	}()
	err = errors.WithMessage(fn(st), "redis业务逻辑出错")
	return
}

// 提供给事务使用
func (rtws *ReloadTWStore) interRemove(c redis.Conn, uid string) error {
	if atomic.LoadInt32(&rtws.reloadStatus) == ReloadEndStatus {
		return rtws.TWStore.interRemove(c, uid)
	}
	return errors.WithMessage(c.Send("LPUSH", rtws.absoluteName, uid), "写入删除队列失败")
}

func (rtws *ReloadTWStore) interRemoveMany(c redis.Conn, uids []string) error {
	if atomic.LoadInt32(&rtws.reloadStatus) == ReloadEndStatus {
		return rtws.TWStore.interRemoveMany(c, uids)
	}
	for _, uid := range uids {
		err := c.Send("LPUSH", rtws.absoluteName, uid)
		if err != nil {
			return errors.WithMessage(err, "批量写入删除队列失败")
		}
	}
	return nil
}
