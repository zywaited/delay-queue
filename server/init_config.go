package server

import (
	"flag"
	"time"

	"github.com/zywaited/delay-queue/inter"
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
