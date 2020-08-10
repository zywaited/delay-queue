package sorted

import (
	"time"

	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role/task"
)

type (
	serverConfig struct {
		// 定时扫描时间
		scale time.Duration

		// 存储
		ns task.NewTaskStore

		logger system.Logger
	}

	ServerConfigOption func(*serverConfig)
)

func ServerConfigWithScale(scale time.Duration) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.scale = scale
	}
}

func ServerConfigWithStore(ns task.NewTaskStore) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.ns = ns
	}
}

func ServerConfigWithLogger(logger system.Logger) ServerConfigOption {
	return func(sc *serverConfig) {
		sc.logger = logger
	}
}
