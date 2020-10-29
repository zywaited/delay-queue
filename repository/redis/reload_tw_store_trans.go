package redis

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
)

type ReloadTWStoreTrans struct {
	*ReloadTWStore

	c redis.Conn
}

// 主要实现修改相关操作
func (st *ReloadTWStoreTrans) Insert(t *model.Task) error {
	return st.interInsert(st.c, t)
}

func (st *ReloadTWStoreTrans) InsertMany(ts []*model.Task) error {
	return st.interInsertMany(st.c, ts)
}

func (st *ReloadTWStoreTrans) Remove(uid string) error {
	return st.interRemove(st.c, uid)
}

func (st *ReloadTWStoreTrans) RemoveMany(uids []string) error {
	return st.interRemoveMany(st.c, uids)
}

func (st *ReloadTWStoreTrans) Status(uid string, tt pb.TaskType) error {
	if tt == pb.TaskType_TaskFinished || tt == pb.TaskType_Ignore {
		return st.interRemove(st.c, uid)
	}
	if tt == pb.TaskType_TaskDelay {
		return st.status(uid, tt, st.c.Send)
	}
	return nil
}

func (st *ReloadTWStoreTrans) NextTime(uid string, nt *time.Time) error {
	return st.nextTime(uid, nt, st.c.Send)
}

func (st *ReloadTWStoreTrans) IncrRetryTimes(uid string, num int) error {
	return st.incrRetryTimes(uid, num, st.c.Send)
}

func (st *ReloadTWStoreTrans) IncrSendTimes(uid string, num int) error {
	return st.incrSendTimes(uid, num, st.c.Send)
}
