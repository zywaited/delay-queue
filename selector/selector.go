package selector

import (
	"fmt"

	"github.com/zywaited/delay-queue/selector/node"
)

type Balancer interface {
	Len() int
	Add(node.Node)
	Remove(node.Node)
	Balance() node.Node
}

type Selector interface {
	Init(Balancer)
	Select(string) (*node.Node, error)
}

type DefaultSelector struct {
}

func (ds *DefaultSelector) Init(bc Balancer) {
}

func (ds *DefaultSelector) Select(serviceName string) (*node.Node, error) {
	return nil, fmt.Errorf("[%s]无可用的服务节点地址", serviceName)
}
