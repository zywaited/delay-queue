package node

type (
	Node struct {
		groupId string // 分组ID
		addr    string // 服务地址（包含端口）
		weight  int    // 权重
		value   []byte // 预留额外信息
	}
)

func NewNode(fields ...Field) *Node {
	n := &Node{}
	for _, field := range fields {
		field(n)
	}
	return n
}

func (n *Node) GetGroupId() string {
	return n.groupId
}

func (n *Node) GetAddr() string {
	return n.addr
}

func (n *Node) GetWeight() int {
	return n.weight
}

func (n *Node) GetValue() []byte {
	return n.value
}
