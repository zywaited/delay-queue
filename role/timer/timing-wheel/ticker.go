package timing_wheel

import "time"

type ticker struct {
	cc *tickerConfig
	// go ticker
	tr *time.Ticker
	// prev timing-wheel ticker
	c chan time.Time
}

type tickerConfig struct {
	t  time.Duration
	st bool
}

type tickerOption func(*tickerConfig)

func tickerOptionWithSt(st bool) tickerOption {
	return func(tc *tickerConfig) {
		tc.st = st
	}
}

func tickerOptionWithTime(t time.Duration) tickerOption {
	return func(tc *tickerConfig) {
		tc.t = t
	}
}

func newTicker(opts ...tickerOption) *ticker {
	c := &tickerConfig{}
	for _, opt := range opts {
		opt(c)
	}
	timer := &ticker{cc: c}
	// 对于底层是定时器，对于上层是推动
	if c.st {
		// 转动时原始刻度
		timer.tr = time.NewTicker(c.t)
		return timer
	}
	timer.c = make(chan time.Time, 1)
	return timer
}

func (t *ticker) stop() {
	if t.cc.st {
		t.tr.Stop()
	}
}

func (t *ticker) ct() <-chan time.Time {
	if t.cc.st {
		return t.tr.C
	}
	return t.c
}

func (t *ticker) up() {
	if t.cc.st {
		return
	}
	t.c <- time.Now()
}
