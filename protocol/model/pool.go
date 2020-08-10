package model

import (
	"sync"
)

var taskPool *sync.Pool

// don't change it
// for zero task
var defaultEmptyTask = Task{}

func init() {
	taskPool = &sync.Pool{New: func() interface{} {
		return &Task{}
	}}
}

func GenerateTask() *Task {
	t := taskPool.Get().(*Task)
	return t
}

func ReleaseTask(t *Task) {
	*t = defaultEmptyTask
	taskPool.Put(t)
}
