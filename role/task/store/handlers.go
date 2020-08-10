package store

import (
	"github.com/zywaited/delay-queue/role/task"
)

type SType string

const DefaultStoreName SType = "task-default-store"

var storeHandlers = make(map[SType]task.NewTaskStore)

func Handler(st SType) task.NewTaskStore {
	if len(storeHandlers) == 0 {
		return nil
	}
	if storeHandlers[st] != nil {
		return storeHandlers[st]
	}
	if storeHandlers[DefaultStoreName] != nil {
		return storeHandlers[DefaultStoreName]
	}
	for _, h := range storeHandlers {
		return h
	}
	return nil
}

func RegisterHandler(st SType, store task.NewTaskStore) {
	storeHandlers[st] = store
}
