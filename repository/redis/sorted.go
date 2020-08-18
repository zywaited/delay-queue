package redis

import (
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
)

// 外部注入 TASK STORE (ZSET)
type SortedSet struct {
	c            *config
	rp           *redis.Pool
	absoluteName string // prefix + "_" + name
	store        role.DataStore
}

func NewSortedSet(rp *redis.Pool, store role.DataStore, opts ...ConfigOption) *SortedSet {
	s := &SortedSet{
		c:     NewConfig(opts...),
		rp:    rp,
		store: store,
	}
	s.absoluteName = s.c.absoluteName()
	return s
}

func (s *SortedSet) Add(t task.Task) error {
	c := s.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	_, err := c.Do("ZADD", s.absoluteName, t.Exec(), t.Uid())
	if err != nil {
		return errors.WithMessagef(err, "写入ZSET集合失败: %s", t.Uid())
	}
	return nil
}

func (s *SortedSet) Remove(uid string) error {
	c := s.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	_, err := c.Do("ZREM", s.absoluteName, uid)
	if err != nil {
		return errors.WithMessagef(err, "删除ZSET集合失败: %s", uid)
	}
	return nil
}

func (s *SortedSet) Get(uid string) task.Task {
	// // ignore error, if need, change get method return error
	mt, err := s.store.Retrieve(uid)
	if err != nil {
		// todo 后续考虑记录
		return nil
	}
	defer model.ReleaseTask(mt)
	return s.c.convert.Convert(mt)
}

func (s *SortedSet) Len() int {
	c := s.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	n, _ := redis.Int(c.Do("ZCARD", s.absoluteName))
	return n
}

func (s *SortedSet) Range(st time.Duration, limit int) []task.Task {
	c := s.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	uids, err := redis.Strings(c.Do("ZRANGEBYSCORE", s.absoluteName, 0, st, "LIMIT", 0, limit))
	if err != nil {
		if err == redis.ErrNil {
			return make([]task.Task, 0)
		}
		// todo 后续考虑记录
		return nil
	}
	mts, err := s.store.Batch(uids)
	if err != nil {
		// todo 后续考虑记录
		return nil
	}
	ts := make([]task.Task, 0, len(mts))
	for _, mt := range mts {
		ts = append(ts, s.c.convert.Convert(mt))
		model.ReleaseTask(mt)
	}
	return ts
}

func (s *SortedSet) MultiRemove(uids []string) error {
	c := s.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	// 组合key，如果后续这个uid太多就采用
	args := make([]interface{}, 0, len(uids)+1)
	args = append(args, s.absoluteName)
	for _, uid := range uids {
		args = append(args, uid)
	}
	_, err := c.Do("ZREM", args...)
	if err != nil {
		return errors.WithMessagef(err, "删除ZSET集合失败: %s", strings.Join(uids, ","))
	}
	return nil
}

func (s *SortedSet) Release() {
}
