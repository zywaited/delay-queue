package redis

import (
	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/protocol/model"
)

type generateLoseStore struct {
	tws *TWStore
}

func NewGenerateLoseStore(tws *TWStore) *generateLoseStore {
	gl := &generateLoseStore{tws: tws}
	return gl
}

func (gl *generateLoseStore) RangeReady(st, et, limit int64) ([]*model.Task, error) {
	tws := gl.tws
	c := tws.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	uids, err := redis.Strings(c.Do("ZRANGEBYSCORE", tws.absoluteName, st, et, "LIMIT", 0, limit))
	if err != nil {
		// nil
		if err == redis.ErrNil {
			return make([]*model.Task, 0), nil
		}
		return nil, errors.WithMessage(err, "redis查询当前分值的任务失败")
	}
	// 这里处理下空数据
	emptyTasks := make([]*model.Task, 0, len(uids))
	mts, err := tws.batchWithEmpty(c, uids, func(uid string) {
		mt := model.GenerateTask()
		mt.Uid = uid
		emptyTasks = append(emptyTasks, mt)
	})
	if err != nil {
		return nil, err
	}
	mts = append(mts, emptyTasks...)
	return mts, nil
}

func (gl *generateLoseStore) ReadyNum(st, et int64) (int64, error) {
	tws := gl.tws
	c := tws.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	l, err := redis.Int64(c.Do("ZCOUNT", tws.absoluteName, st, et))
	if err != nil {
		if err == redis.ErrNil {
			return l, nil
		}
		return l, errors.WithMessage(err, "redis查询当前分值的数量")
	}
	return l, nil
}

func (gl *generateLoseStore) NextReady(st, et, limit int64) (int64, error) {
	tws := gl.tws
	c := tws.rp.Get()
	defer func() {
		_ = c.Close()
	}()
	score, err := redis.Values(c.Do("ZRANGEBYSCORE", tws.absoluteName, st, et, "WITHSCORES", "LIMIT", limit-1, 1))
	if err != nil {
		// nil
		if err == redis.ErrNil {
			return et, nil
		}
		return 0, errors.WithMessage(err, "redis查询当前分值的任务失败")
	}
	if len(score) < 2 {
		return et, nil
	}
	max, err := redis.Float64(score[1], nil)
	if err != nil {
		if err == redis.ErrNil {
			return et, nil
		}
		return 0, errors.WithMessage(err, "redis查询当前分值的任务失败")
	}
	return int64(max), nil
}
