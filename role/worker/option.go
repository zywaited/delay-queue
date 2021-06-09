package worker

import (
	"time"

	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/limiter"
	"github.com/zywaited/delay-queue/role/runner"
	"github.com/zywaited/delay-queue/transport"
)

type option struct {
	retryTimes    int
	timeout       time.Duration
	repeatedTimes int
	configScale   time.Duration
	rq            runner.ReadyQueue
	logger        system.Logger
	store         role.DataStore
	ts            transport.TransporterM
	gp            limiter.Pool
}

type Options func(*option)

func OptionsWithRetryTimes(retryTimes int) Options {
	return func(op *option) {
		op.retryTimes = retryTimes
	}
}

func OptionsWithTimeout(timeout time.Duration) Options {
	return func(op *option) {
		op.timeout = timeout
	}
}

func OptionsWithRepeatedTimes(repeatedTimes int) Options {
	return func(op *option) {
		op.repeatedTimes = repeatedTimes
	}
}

func OptionsWithConfigScale(configScale time.Duration) Options {
	return func(op *option) {
		op.configScale = configScale
	}
}

func OptionsWithReadyQueue(rq runner.ReadyQueue) Options {
	return func(op *option) {
		op.rq = rq
	}
}

func OptionWithLogger(logger system.Logger) Options {
	return func(op *option) {
		op.logger = logger
	}
}

func OptionWithStore(store role.DataStore) Options {
	return func(op *option) {
		op.store = store
	}
}

func OptionWithTransporters(ts transport.TransporterM) Options {
	return func(op *option) {
		for t, tr := range ts {
			op.ts[t] = tr
		}
	}
}

func OptionWithGP(gp limiter.Pool) Options {
	return func(op *option) {
		op.gp = gp
	}
}
