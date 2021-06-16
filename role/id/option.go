package id

import (
	"time"

	"github.com/zywaited/delay-queue/parser/system"
)

type Options func(*option)

type option struct {
	exitFunc func()
	logger   system.Logger

	timerId  string
	workerId string
	nodeId   int64

	prefix       string
	hash         string
	validTime    int
	checkTime    int
	internalTime time.Duration
	cacheNum     int64
}

func NewOption(opts ...Options) *option {
	op := &option{}
	for _, opt := range opts {
		opt(op)
	}
	return op
}

func OptionsWithExitFunc(fn func()) Options {
	return func(op *option) {
		op.exitFunc = fn
	}
}

func OptionsWithTimerId(id string) Options {
	return func(op *option) {
		op.timerId = id
	}
}

func OptionsWithWorkerId(id string) Options {
	return func(op *option) {
		op.workerId = id
	}
}

func OptionsWithNodeId(id int64) Options {
	return func(op *option) {
		op.nodeId = id
	}
}

func OptionsWithPrefix(prefix string) Options {
	return func(op *option) {
		op.prefix = prefix
	}
}

func OptionsWithHashKey(key string) Options {
	return func(op *option) {
		op.hash = key
	}
}

func OptionsWithValidTime(t int) Options {
	return func(op *option) {
		op.validTime = t
	}
}

func OptionsWithCheckTime(t int) Options {
	return func(op *option) {
		op.checkTime = t
	}
}

func OptionsWithCacheNum(n int64) Options {
	return func(op *option) {
		op.cacheNum = n
	}
}

func OptionsWithLogger(logger system.Logger) Options {
	return func(op *option) {
		op.logger = logger
	}
}

func OptionsWithInternalTime(t time.Duration) Options {
	return func(op *option) {
		op.internalTime = t
	}
}
