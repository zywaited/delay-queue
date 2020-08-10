package store

import (
	"sync"

	"github.com/zywaited/delay-queue/role/task"
)

type (
	taskStore interface {
		task.Store

		reset(pool)
	}

	pool interface {
		NewTaskStore() task.Store
		ReleaseTaskStore(task.Store)

		newTaskStoreIter() task.StoreIterator
		releaseTaskStoreIter(task.StoreIterator)
	}

	newTaskStore     func(pool) task.Store
	newTaskStoreIter func(pool) task.StoreIterator
)

type TaskPoolStore struct {
	storePool *sync.Pool
	iterPool  *sync.Pool
}

func NewTaskPoolStore(sn newTaskStore, in newTaskStoreIter) *TaskPoolStore {
	tpl := &TaskPoolStore{}
	tpl.storePool = &sync.Pool{New: func() interface{} {
		return sn(tpl)
	}}
	tpl.iterPool = &sync.Pool{New: func() interface{} {
		return in(tpl)
	}}
	return tpl
}

func (tpl *TaskPoolStore) NewTaskStore() task.Store {
	tl := tpl.storePool.Get().(taskStore)
	tl.reset(tpl)
	return tl
}

func (tpl *TaskPoolStore) ReleaseTaskStore(tk task.Store) {
	tpl.storePool.Put(tk)
}

func (tpl *TaskPoolStore) newTaskStoreIter() task.StoreIterator {
	return tpl.iterPool.Get().(task.StoreIterator)
}

func (tpl *TaskPoolStore) releaseTaskStoreIter(iter task.StoreIterator) {
	tpl.iterPool.Put(iter)
}
