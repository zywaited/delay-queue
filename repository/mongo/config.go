package mongo

import (
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/go-common/xcopy"
)

type config struct {
	token      string
	db         string
	collection string
	cp         xcopy.XCopy
	convert    role.PbConvertTask
}

type ConfigOption func(*config)

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

func ConfigWithToken(token string) ConfigOption {
	return func(c *config) {
		c.token = token
	}
}

func ConfigWithDb(db string) ConfigOption {
	return func(c *config) {
		c.db = db
	}
}

func ConfigWithCollection(collection string) ConfigOption {
	return func(c *config) {
		c.collection = collection
	}
}

func NewConfig(opts ...ConfigOption) *config {
	c := &config{}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
