package redis

import (
	"fmt"

	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/go-common/xcopy"
)

const (
	DefaultStorePrefix = "med-delay-queue"
	DefaultStoreName   = "timing-task"
)

type config struct {
	prefix string
	name   string
	id     string

	cp      xcopy.XCopy // 仅仅是为了内部尽量减少全局变量依赖
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

func ConfigWithCopy(cp xcopy.XCopy) ConfigOption {
	return func(c *config) {
		c.cp = cp
	}
}

func ConfigWithConvert(convert role.PbConvertTask) ConfigOption {
	return func(c *config) {
		c.convert = convert
	}
}

func ConfigWithId(id string) ConfigOption {
	return func(c *config) {
		c.id = id
	}
}

func NewConfig(opts ...ConfigOption) *config {
	c := &config{
		prefix: DefaultStorePrefix,
		name:   DefaultStoreName,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *config) absoluteName() string {
	if c.id == "" {
		return fmt.Sprintf("%s_%s", c.prefix, c.name)
	}
	return fmt.Sprintf("%s_%s_%s", c.prefix, c.name, c.id)
}

func (c *config) storeName() string {
	return c.prefix + "_" + c.name
}
