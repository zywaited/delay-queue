package id

import (
	"context"
	"errors"

	"github.com/bwmarrin/snowflake"
	"github.com/zywaited/delay-queue/role"
)

type localId struct {
	op *option
	sn *snowflake.Node
}

func NewNodeId(opts ...Options) *localId {
	op := NewOption(opts...)
	sn, _ := snowflake.NewNode(op.nodeId)
	return &localId{op: op, sn: sn}
}

func (c *localId) Run() error {
	if c.sn == nil {
		return errors.New("node id")
	}
	return nil
}

func (c *localId) Stop(_ role.StopType) error {
	return nil
}

func (c *localId) Timer(_ context.Context) (string, error) {
	return c.op.timerId, nil
}

func (c *localId) Worker(_ context.Context) (string, error) {
	return c.op.workerId, nil
}

func (c *localId) Id(context.Context) (int64, error) {
	return c.sn.Generate().Int64(), nil
}
