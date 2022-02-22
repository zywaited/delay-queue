package runner

import (
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/role/task"
)

type memoryReadyQueue struct {
	factory task.Factory
	tc      chan task.Task
}

func NewMemoryReadyQueue(factory task.Factory) *memoryReadyQueue {
	return &memoryReadyQueue{factory: factory, tc: make(chan task.Task, 128)}
}

func (m *memoryReadyQueue) Push(t task.Task) error {
	//m.tc <- t
	// fix: 拷贝一份出来
	m.tc <- m.factory(
		task.ParamWithUid(t.Uid()),
		task.ParamWithName(t.Name()),
		task.ParamWithType(t.Type()),
	)
	return nil
}

func (m *memoryReadyQueue) Pop() (t task.Task, err error) {
	select {
	case t = <-m.tc:
	default:
		err = xerror.WithXCodeMessage(xcode.DBRecordNotFound, "任务不存在")
	}
	return
}

func (m *memoryReadyQueue) Len() int {
	return len(m.tc)
}
