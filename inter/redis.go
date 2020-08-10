package inter

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	pkgerror "github.com/pkg/errors"
)

func init() {
	configAfters = append(configAfters, ConfigDataWithRedis)
}

func ConfigDataWithRedis(cd *ConfigData) error {
	redisConfig := cd.C.Redis
	if redisConfig == nil {
		return errors.New("redis's config is error")
	}
	// 尝试是否可以连接
	dial := func() (redis.Conn, error) {
		return redis.Dial(
			"tcp",
			redisConfig.Addr,
			redis.DialPassword(redisConfig.Auth),
			redis.DialDatabase(redisConfig.Db),
			redis.DialConnectTimeout(time.Duration(redisConfig.ConnectTimeout)*time.Millisecond),
		)
	}
	c, err := dial()
	if err != nil {
		return pkgerror.WithMessage(err, "init redis failed")
	}
	_ = c.Close()
	cd.CB.Redis = &redis.Pool{
		Dial:      dial,
		MaxIdle:   redisConfig.Idle,
		MaxActive: redisConfig.Active,
		Wait:      redisConfig.Wait,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err = c.Do("PING")
			return pkgerror.WithMessagef(err, "redis's socket invalid")
		},
	}
	return nil
}
