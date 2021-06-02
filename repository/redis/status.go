package redis

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/protocol/pb"
)

type statusStore struct {
	*mapStore
}

func NewStatusStore(rp *redis.Pool, opts ...ConfigOption) *statusStore {
	return &statusStore{mapStore: NewMapStore(rp, opts...)}
}

func (s *statusStore) Status(uid string, tt pb.TaskType) error {
	// redis数据直接删除即可
	if tt == pb.TaskType_TaskFinished || tt == pb.TaskType_Ignore {
		return s.mapStore.Remove(uid)
	}
	// 只有delay状态要变更
	if tt == pb.TaskType_TaskDelay {
		return s.status(uid, tt, s.mapStore.do)
	}
	return nil
}

func (s *statusStore) status(uid string, tt pb.TaskType, send Send) error {
	key := fmt.Sprintf("%s_%s", s.mapStore.absoluteName, uid)
	return errors.WithMessage(send("HSET", key, "type", int32(tt)), "Redis数据更新状态失败")
}

func (s *statusStore) NextTime(uid string, nt *time.Time) error {
	return s.nextTime(uid, nt, s.mapStore.do)
}

func (s *statusStore) nextTime(uid string, nt *time.Time, send Send) error {
	key := fmt.Sprintf("%s_%s", s.mapStore.absoluteName, uid)
	return errors.WithMessage(send("HSET", key, "next_exec_time", nt.UnixNano()), "Redis数据更新下次执行时间失败")
}

func (s *statusStore) IncrRetryTimes(uid string, num int) error {
	return s.incrRetryTimes(uid, num, s.mapStore.do)
}

func (s *statusStore) incrRetryTimes(uid string, num int, send Send) error {
	key := fmt.Sprintf("%s_%s", s.mapStore.absoluteName, uid)
	return errors.WithMessage(send("HINCRBY", key, "retry_times", num), "Redis数据更新重试次数失败")
}

func (s *statusStore) IncrSendTimes(uid string, num int) error {
	return s.incrSendTimes(uid, num, s.mapStore.do)
}

func (s *statusStore) incrSendTimes(uid string, num int, send Send) error {
	key := fmt.Sprintf("%s_%s", s.mapStore.absoluteName, uid)
	return errors.WithMessage(send("HINCRBY", key, "times", num), "Redis数据更新发送次数失败")
}
