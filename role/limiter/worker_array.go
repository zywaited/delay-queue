package limiter

import (
	"sort"
	"time"
)

// 数组存储
type workerArray struct {
	ws []Worker
}

func NewWorkerArray() *workerArray {
	return &workerArray{}
}

func (wa *workerArray) Len() int {
	return len(wa.ws)
}

func (wa *workerArray) Add(w Worker) {
	wa.ws = append(wa.ws, w)
}

func (wa *workerArray) AcquireOne() Worker {
	if wa.Len() == 0 {
		return nil
	}
	w := wa.ws[0]
	wa.ws = wa.ws[1:]
	return w
}

func (wa *workerArray) Acquire(max int) []Worker {
	if wa.Len() < max {
		max = wa.Len()
	}
	ws := wa.ws[:max]
	wa.ws = wa.ws[max:]
	return ws
}

func (wa *workerArray) Expire(t time.Duration, max int) []Worker {
	if wa.Len() < max {
		max = wa.Len()
	}
	index := sort.Search(max, func(i int) bool {
		return !wa.ws[i].Expire(t)
	})
	ws := wa.ws[:index]
	wa.ws = wa.ws[index:]
	return ws
}
