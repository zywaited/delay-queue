package id

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
	pkgerr "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
)

const (
	internalTime = time.Millisecond * 10
	validTime    = 5
	maxLostNum   = 5
)

type redisId struct {
	op        *option
	rp        *redis.Pool
	currentId int64
	maxId     int64
	wait      int64
	keys      string
	lockKey   string
	idKey     string
	version   int64
	ir        chan []int64
	wr        chan int64
	ws        chan struct{}
	sc        chan struct{}
	ec        chan struct{}
}

func NewRedisId(rp *redis.Pool, opts ...Options) *redisId {
	r := &redisId{
		rp: rp,
		op: NewOption(opts...),
		wr: make(chan int64, 128),
		ws: make(chan struct{}),
		ir: make(chan []int64),
		ec: make(chan struct{}, 1),
	}
	r.op.cacheNum += 128
	if r.op.internalTime <= 0 {
		r.op.internalTime = internalTime
	}
	if r.op.validTime <= 0 {
		r.op.validTime = validTime
	}
	if r.op.checkTime <= 0 || r.op.checkTime >= r.op.validTime {
		r.op.checkTime = r.op.validTime - int(math.Ceil(float64(r.op.validTime)/3.0))
	}
	r.currentId = 1
	// hash {node: version}
	r.keys = r.acquireKeys()
	return r
}

func (r *redisId) Run() error {
	err := r.lock()
	if err != nil {
		return err
	}
	go r.lease()
	go r.run()
	go r.acquireIds()
	return nil
}

func (r *redisId) Stop(_ role.StopType) error {
	r.stop()
	return nil
}

func (r *redisId) Timer(_ context.Context) (string, error) {
	return r.op.timerId, nil
}

func (r *redisId) Worker(_ context.Context) (string, error) {
	return r.op.workerId, nil
}

func (r *redisId) Id(ctx context.Context) (int64, error) {
	atomic.AddInt64(&r.wait, 1)
	defer atomic.AddInt64(&r.wait, -1)
	select {
	case id := <-r.wr:
		return id, nil
	case <-ctx.Done():
		return 0, role.ErrContextCancel
	case <-r.sc:
		return 0, role.ErrServerExit
	}
}

func (r *redisId) run() {
	r.sc = make(chan struct{})
	defer func() {
		if r.op.exitFunc != nil {
			r.op.exitFunc()
		}
		close(r.sc)
		rerr := recover()
		if rerr == nil || r.op.logger == nil {
			return
		}
		r.op.logger.Infof("redis run panic: %v, stack: %s", rerr, system.Stack())
	}()
	var (
		remote        = 1
		wr            chan int64
		ws            = r.ws
		ir            = r.ir
		nextCurrentId int64
		nextMaxId     int64
	)
	for {
		wr = r.wr
		ws = nil
		ir = nil
		if (r.currentId > r.maxId) && nextCurrentId < nextMaxId {
			// 这里主从复制延迟阔能会导致数据不一致
			// 虽然数据库有唯一键，但这里也可以处理下
			if nextMaxId >= r.currentId {
				r.maxId = nextMaxId
				if nextCurrentId > r.currentId {
					r.currentId = nextCurrentId
				}
			}
			if nextMaxId < r.currentId {
				// 重新读
				remote = 1
			}
			nextCurrentId = 0
			nextMaxId = 0
		}
		switch remote {
		case 1:
			ws = r.ws
		case 2:
			ir = r.ir
		}
		if r.currentId > r.maxId {
			wr = nil
		}
		select {
		case ws <- struct{}{}:
			remote = 2
		case ids := <-ir:
			nextCurrentId = ids[0]
			nextMaxId = ids[1]
			remote = 0
		case wr <- r.currentId:
			r.currentId++
			if remote == 0 &&
				nextCurrentId == 0 &&
				r.maxId-r.currentId <= int64(math.Ceil(float64(r.op.cacheNum)/5.0)) {
				remote = 1
			}
		case <-r.ec:
			return
		}
	}
}

func (r *redisId) acquireIds() {
	defer func() {
		rerr := recover()
		r.stop()
		if rerr == nil || r.op.logger == nil {
			return
		}
		r.op.logger.Infof("redis acquire ids panic: %v, stack: %s", rerr, system.Stack())
	}()
	generateIds := func() []int64 {
		c := r.rp.Get()
		defer c.Close()
		spanNum := r.op.cacheNum + atomic.LoadInt64(&r.wait)
		// note: redigo.NewScript
		num, err := redis.Int64(c.Do("EVAL", `
-- 判断是否存在锁，不存在就添加 --
local exist = redis.call("EXISTS", KEYS[1])
local maxVersion = redis.call("HGET", KEYS[3], ARGV[4])
if (exist == 0 and maxVersion == ARGV[1])
then
	redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
end
-- 防止版本升级丢失后又重新获取 --
if (maxVersion ~= ARGV[1])
then
	return 0
end
-- ID区间增加 --
return redis.call("INCRBY", KEYS[2], ARGV[3])
`,
			3,
			r.lockKey,
			r.idKey,
			r.keys,
			r.version,
			r.op.validTime,
			spanNum,
			r.op.timerId,
		))
		if err == nil {
			return []int64{num - spanNum + 1, num}
		}
		if r.op.logger != nil {
			r.op.logger.Infof("redis acquire ids err: %v", err)
		}
		return nil
	}
	for {
		select {
		case <-r.ws:
		case <-r.sc:
			return
		}
		for {
			ids := generateIds()
			if len(ids) == 0 {
				time.Sleep(r.op.internalTime)
				continue
			}
			if ids[0] <= 0 || ids[0] >= ids[1] {
				if r.op.logger != nil {
					r.op.logger.Info("redis acquire ids invalid")
				}
				return
			}
			r.ir <- ids
			break
		}
	}
}

func (r *redisId) lock() error {
	// note:
	// 抢占Key，后续可以搞个支持分配的服务
	c := r.rp.Get()
	defer c.Close()
	cursor := -1
	args := make([]interface{}, 0, 20)
	for cursor != 0 {
		if cursor < 0 {
			cursor = 0
		}
		vs, err := redis.Values(c.Do("HSCAN", r.keys, cursor, "COUNT", 10))
		if err != nil {
			return pkgerr.WithMessage(err, "redis acquire key failed")
		}
		cursor, err = redis.Int(vs[0], err)
		if err != nil {
			return pkgerr.WithMessage(err, "redis acquire key cursor failed")
		}
		keys, err := redis.Int64Map(vs[1], err)
		if err != nil {
			return pkgerr.WithMessage(err, "redis acquire keys failed")
		}
		if len(keys) == 0 {
			continue
		}
		// 通用逻辑
		// 先整体获取未使用的key，再逐个加锁
		// KEYS数组在部分云redis上不能用表达式
		args = args[:0]
		fields := make([]string, 0, len(keys))
		for key := range keys {
			fields = append(fields, key)
			args = append(args, r.acquireLockKey(key))
		}
		ks, err := redis.Int64s(c.Do("MGET", args...))
		km := make(map[int]bool, len(ks))
		for i, k := range ks {
			km[i] = k > 0
		}
		for i, field := range fields {
			if km[i] {
				continue
			}
			args = args[:0]
			args = append(args, `
if (redis.call("EXISTS", KEYS[2]) == 0)
then
	local version = redis.call("HINCRBY", KEYS[1], ARGV[2], 1)
	redis.call("SET", KEYS[2], version, "EX", ARGV[1])
	return version
end
return 0
`)
			args = append(args, 2)
			args = append(args, r.keys, r.acquireLockKey(field))
			args = append(args, r.op.validTime, field)
			version, err := redis.Int64(c.Do("EVAL", args...))
			if err != nil {
				return pkgerr.WithMessagef(err, "redis acquire keys failed: %s", field)
			}
			if version == 0 {
				time.Sleep(r.op.internalTime)
				continue
			}
			r.version = version
			r.op.timerId = field
			r.idKey = r.acquireIdKey(field)
			r.lockKey = r.acquireLockKey(field)
			return nil
		}
	}
	return errors.New("redis acquire key failed: invalid")
}

func (r *redisId) lease() {
	lt := time.NewTicker(time.Second * time.Duration(r.op.checkTime))
	defer func() {
		rerr := recover()
		lt.Stop()
		r.stop()
		r.remove()
		if rerr == nil || r.op.logger == nil {
			return
		}
		r.op.logger.Infof("redis lease panic: %v, stack: %s", rerr, system.Stack())
	}()
	lease := func() int {
		c := r.rp.Get()
		defer c.Close()
		// note: redigo.NewScript
		success, err := redis.Int(c.Do("EVAL", `
local exist = redis.call("EXISTS", KEYS[1])
local maxVersion = redis.call("HGET", KEYS[2], ARGV[3])
if ((exist == 0 and maxVersion == ARGV[1]) or (exist == 1 and maxVersion == ARGV[1]))
then
	redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
	return 1
end
return -1
`,
			2,
			r.lockKey,
			r.keys,
			r.version,
			r.op.validTime,
			r.op.timerId,
		))
		if err == nil {
			return success
		}
		if r.op.logger != nil {
			r.op.logger.Infof("redis lease err: %v", err)
		}
		return 0
	}
	for {
		select {
		case <-r.sc:
			return
		case <-lt.C:
		}
		currentNum := 0
		for {
			success := lease()
			if success == 0 {
				if currentNum < r.op.maxLostNum {
					time.Sleep(r.op.internalTime << currentNum)
					currentNum++
					continue
				}
				success = -1
			}
			if success < 0 {
				if r.op.logger != nil {
					r.op.logger.Info("redis lease version invalid")
				}
				return
			}
			break
		}
	}
}

func (r *redisId) remove() {
	c := r.rp.Get()
	_, _ = c.Do("EVAL", `
local exist = redis.call("EXISTS", KEYS[1])
local maxVersion = redis.call("HGET", KEYS[2], ARGV[2])
if (exist == 1 and maxVersion == ARGV[1])
then
	-- 利用一个版本防止冲突 --
	redis.call("HINCRBY", KEYS[2], ARGV[2], 1)
	redis.call("DEL", KEYS[1])
end
return 1
`,
		2,
		r.lockKey,
		r.keys,
		r.op.timerId,
	)
	_ = c.Close()
}

func (r *redisId) acquireLockKey(field string) string {
	if r.op.hash == "" {
		return fmt.Sprintf("%s_%s_lock", r.op.prefix, field)
	}
	return fmt.Sprintf("%s_%s_lock{%s}", r.op.prefix, field, r.op.hash)
}

func (r *redisId) acquireIdKey(field string) string {
	if r.op.hash == "" {
		return fmt.Sprintf("%s_%s_ids", r.op.prefix, field)
	}
	return fmt.Sprintf("%s_%s_ids{%s}", r.op.prefix, field, r.op.hash)
}

func (r *redisId) acquireKeys() string {
	if r.op.hash == "" {
		return fmt.Sprintf("%s_keys", r.op.prefix)
	}
	return fmt.Sprintf("%s_keys{%s}", r.op.prefix, r.op.hash)
}

func (r *redisId) stop() {
	select {
	case r.ec <- struct{}{}:
	default:
	}
}
