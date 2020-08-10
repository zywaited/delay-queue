package node

type Field func(*Node)

func WithGroupId(groupId string) Field {
	return func(n *Node) {
		n.groupId = groupId
	}
}

func WithAddr(addr string) Field {
	return func(n *Node) {
		n.addr = addr
	}
}

func WithWeight(weight int) Field {
	return func(n *Node) {
		n.weight = weight
	}
}

func WithValue(value []byte) Field {
	return func(n *Node) {
		n.value = value
	}
}
