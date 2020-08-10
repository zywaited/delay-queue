package store

import (
	"time"

	"github.com/zywaited/delay-queue/role/task"
)

const SortedListStoreName SType = "task-sorted-list-store"

type SortedList interface {
	task.StoreBaser

	/**
	 * @param et time.Duration 截止时间
	 * @param limit int 限制数量
	 */
	Range(time.Duration, int) []task.Task
}

// 时间排序
type sortedList struct {
	s SortedList

	limit int
	// 为了不每次去读取redis
	num int

	p pool
}

type SLOption func(*sortedList)

const defaultSLILimit = 50

func SLOptionWithLimit(limit int) SLOption {
	return func(sl *sortedList) {
		sl.limit = limit
	}
}

func NewSortedList(s SortedList, opts ...SLOption) *sortedList {
	sl := &sortedList{s: s, limit: defaultSLILimit, num: -1}
	for _, opt := range opts {
		opt(sl)
	}
	return sl
}

// 用于生成数据(pool)
func (sl *sortedList) NewSortedList(p pool) task.Store {
	return &sortedList{s: sl.s, limit: sl.limit, num: -1, p: p}
}

func (sl *sortedList) reset(p pool) {
	sl.p = p
}

func (sl *sortedList) Len() int {
	if sl.num == -1 {
		sl.num = sl.s.Len()
	}
	return sl.num
}

func (sl *sortedList) Add(t task.Task) error {
	return sl.s.Add(t)
}

func (sl *sortedList) Remove(uid string) error {
	return sl.s.Remove(uid)
}

func (sl *sortedList) Get(uid string) task.Task {
	return sl.s.Get(uid)
}

func (sl *sortedList) Iter() task.StoreIterator {
	ts := sl.s.Range(time.Duration(time.Now().UnixNano()), sl.limit)
	iter := sl.p.newTaskStoreIter().(*sortedListIter)
	iter.reset(sl.p, ts)
	return iter
}

func (sl *sortedList) Release() {
	p := sl.p
	sl.p = nil
	sl.s.Release()
	p.ReleaseTaskStore(sl)
}

type sortedListIter struct {
	p     pool
	ts    []task.Task
	index int
}

func NewSortedListStoreIterator(p pool) task.StoreIterator {
	return &sortedListIter{p: p}
}

func (sli *sortedListIter) reset(p pool, ts []task.Task) {
	sli.p = p
	sli.ts = ts
	sli.index = 0
}

func (sli *sortedListIter) Len() int {
	return len(sli.ts)
}

func (sli *sortedListIter) Next() bool {
	return sli.index < sli.Len()
}

func (sli *sortedListIter) Rewind() {
	sli.index = 0
}

func (sli *sortedListIter) Release() {
	p := sli.p
	sli.ts = nil
	sli.p = nil
	p.releaseTaskStoreIter(sli)
}

func (sli *sortedListIter) Scan() task.Task {
	t := sli.ts[sli.index]
	sli.index++
	return t
}
