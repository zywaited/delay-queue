package server

import (
	"flag"
	"math"
	"time"

	"github.com/zywaited/delay-queue/inter"
	"github.com/zywaited/go-common/limiter"
)

type configInitOption struct {
}

func NewConfigInitOption() *configInitOption {
	return &configInitOption{}
}

func (co *configInitOption) Run(dq *DelayQueue) error {
	configFile := ""
	flag.StringVar(&configFile, "f", "config/config.toml", "config file")
	flag.Parse()
	gc := inter.NewGenerateConfigWithDefaultAfters(inter.GenerateConfigWithConfigName(configFile))
	if err := gc.Run(); err != nil {
		return err
	}
	dq.c = gc.ConfigData()
	return nil
}

func (co *configInitOption) Stop(_ *DelayQueue) error {
	return nil
}

type baseLevelOption struct {
}

func NewBaseLevelOption() *baseLevelOption {
	return &baseLevelOption{}
}

func (blo *baseLevelOption) Run(dq *DelayQueue) error {
	switch dq.c.C.BaseLevel {
	case "second":
		dq.base = time.Second
	}
	return nil
}

func (blo *baseLevelOption) Stop(_ *DelayQueue) error {
	return nil
}

type poolOption struct {
}

func NewPoolOption() *poolOption {
	return &poolOption{}
}

func (po *poolOption) Run(dq *DelayQueue) error {
	opts := []limiter.PoolOptions{limiter.PoolOptionsWithLogger(dq.c.CB.Logger)}
	if dq.c.C.Gp != nil && dq.c.C.Gp.Limit > 0 {
		opts = append(opts, limiter.PoolOptionsWithLimit(dq.c.C.Gp.Limit))
	}
	if dq.c.C.Gp != nil && dq.c.C.Gp.Idle > 0 {
		opts = append(opts, limiter.PoolOptionsWithIdle(dq.c.C.Gp.Idle))
	}
	if dq.c.C.Gp != nil && dq.c.C.Gp.IdleTime > 0 {
		opts = append(opts, limiter.PoolOptionsWithIdleTime(dq.base*time.Duration(dq.c.C.Gp.IdleTime)))
	}
	if dq.c.C.Gp != nil && dq.c.C.Gp.CheckNum > 0 {
		opts = append(opts, limiter.PoolOptionsWithCheckNum(dq.c.C.Gp.CheckNum))
	}
	if dq.c.C.Gp != nil && dq.c.C.Gp.BlockTime != 0 {
		multi := float64(dq.c.C.Gp.BlockTime)
		if dq.c.C.Gp.BlockTime < 0 {
			multi = 1.0 / float64(-dq.c.C.Gp.BlockTime)
		}
		opts = append(opts, limiter.PoolOptionsWithBlockTime(time.Duration(math.Ceil(float64(dq.base)*multi))))
	}
	dq.gp = limiter.NewPool(limiter.NewWorkerArray(), opts...)
	return nil
}

func (po *poolOption) Stop(_ *DelayQueue) error {
	return nil
}
