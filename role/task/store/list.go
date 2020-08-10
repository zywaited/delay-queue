package store

import (
	"container/list"

	"github.com/zywaited/delay-queue/role/task"
)

const ListStoreName SType = "task-list-store"

var taskListPool = NewTaskPoolStore(newTaskListStore, newTaskListStoreIterator)

func init() {
	RegisterHandler(ListStoreName, taskListPool.NewTaskStore)
	RegisterHandler(DefaultStoreName, taskListPool.NewTaskStore)
}

type TaskList struct {
	tls pool

	data *list.List
	em   map[string]*list.Element // 为了删除O(1)
}

func newTaskListStore(tls pool) task.Store {
	return newTaskList(tls)
}

func newTaskList(tls pool) *TaskList {
	return &TaskList{
		tls:  tls,
		data: list.New(),
		em:   make(map[string]*list.Element),
	}
}

// 只在pool中使用
func (tl *TaskList) reset(tls pool) {
	tl.tls = tls
	tl.data.Init()
	tl.em = make(map[string]*list.Element)
}

func (tl *TaskList) Len() int {
	return tl.data.Len()
}

func (tl *TaskList) Add(t task.Task) error {
	// 是否已经存在了
	e := tl.em[t.Uid()]
	if e == nil {
		tl.em[t.Uid()] = tl.data.PushBack(t)
		return nil
	}
	e.Value = t
	return nil
}

func (tl *TaskList) Remove(uid string) error {
	e := tl.em[uid]
	if e == nil {
		return nil
	}
	delete(tl.em, uid)
	tl.data.Remove(e)
	return nil
}

func (tl *TaskList) Get(uid string) task.Task {
	e := tl.em[uid]
	if e == nil {
		return nil
	}
	return e.Value.(task.Task)
}

func (tl *TaskList) Release() {
	// GC
	tls := tl.tls
	tl.tls = nil
	tl.em = nil
	tl.data.Init()
	tls.ReleaseTaskStore(tl)
}

func (tl *TaskList) Iter() task.StoreIterator {
	iter := tl.tls.newTaskStoreIter().(*TaskListStoreIterator)
	iter.reset(tl)
	return iter
}

type TaskListStoreIterator struct {
	tls pool
	tl  *TaskList
	e   *list.Element
}

func newTaskListStoreIterator(tls pool) task.StoreIterator {
	return newTaskListIterator(tls)
}

func newTaskListIterator(tls pool) *TaskListStoreIterator {
	return &TaskListStoreIterator{
		tls: tls,
	}
}

// 内部store使用
func (iter *TaskListStoreIterator) reset(tl *TaskList) {
	iter.tls = tl.tls
	iter.tl = tl
	iter.e = tl.data.Front()
}

func (iter *TaskListStoreIterator) Len() int {
	return iter.tl.Len()
}

func (iter *TaskListStoreIterator) Next() bool {
	return iter.e != nil
}

func (iter *TaskListStoreIterator) Scan() task.Task {
	if iter.e == nil {
		return nil
	}
	e := iter.e
	iter.e = e.Next()
	return e.Value.(task.Task)
}

func (iter *TaskListStoreIterator) Rewind() {
	iter.e = iter.tl.data.Front()
}

func (iter *TaskListStoreIterator) Release() {
	// GC
	tls := iter.tls
	iter.tls = nil
	iter.tl = nil
	iter.e = nil
	tls.releaseTaskStoreIter(iter)
}
