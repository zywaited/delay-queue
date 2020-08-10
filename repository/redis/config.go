package redis

import (
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/go-common/xcopy"
)

const (
	DefaultSortedSetPrefix = "med-delay-queue"
	DefaultSortedSetName   = "timing-task"
)

type config struct {
	prefix string
	name   string

	cp      *xcopy.XCopy // 仅仅是为了内部尽量减少全局变量依赖
	convert role.PbConvertTask
}

type ConfigOption func(*config)

func ConfigWithPrefix(prefix string) ConfigOption {
	return func(c *config) {
		c.prefix = prefix
	}
}

func ConfigWithName(name string) ConfigOption {
	return func(c *config) {
		c.name = name
	}
}

func ConfigWithCopy(cp *xcopy.XCopy) ConfigOption {
	return func(c *config) {
		c.cp = cp
	}
}

func ConfigWithConvert(convert role.PbConvertTask) ConfigOption {
	return func(c *config) {
		c.convert = convert
	}
}

func NewConfig(opts ...ConfigOption) *config {
	c := &config{
		prefix: DefaultSortedSetPrefix,
		name:   DefaultSortedSetName,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
