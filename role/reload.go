package role

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/role/task"
)

type generateLoseTask struct {
	gls GenerateLoseStore
	c   PbConvertTask

	st    int64
	et    int64
	limit int64
}

type GenerateLoseTaskOption func(rt *generateLoseTask)

func GenerateLoseTaskOptionWithSt(st int64) GenerateLoseTaskOption {
	return func(rt *generateLoseTask) {
		rt.st = st
	}
}

func GenerateLoseTaskOptionWithEt(et int64) GenerateLoseTaskOption {
	return func(rt *generateLoseTask) {
		rt.et = et
	}
}

func GenerateLoseTaskOptionWithLimit(limit int64) GenerateLoseTaskOption {
	return func(rt *generateLoseTask) {
		rt.limit = limit
	}
}

func GenerateLoseTaskOptionWithGenerateLoseStore(gls GenerateLoseStore) GenerateLoseTaskOption {
	return func(rt *generateLoseTask) {
		rt.gls = gls
	}
}

func GenerateLoseTaskOptionWithPbConvertTask(c PbConvertTask) GenerateLoseTaskOption {
	return func(rt *generateLoseTask) {
		rt.c = c
	}
}

func NewGenerateLoseTask(opts ...GenerateLoseTaskOption) *generateLoseTask {
	rt := &generateLoseTask{limit: -1}
	rt.init(opts...)
	return rt
}

func (rt *generateLoseTask) init(opts ...GenerateLoseTaskOption) {
	for _, opt := range opts {
		opt(rt)
	}
}

func (rt *generateLoseTask) reset() {
	rt.gls = nil
	rt.c = nil
}

func (rt *generateLoseTask) Len() (int64, error) {
	l, err := rt.gls.ReadyNum(rt.st, rt.et)
	return l, errors.WithMessage(err, "reload task error")
}

func (rt *generateLoseTask) Reload() ([]task.Task, error) {
	var ts []task.Task
	if rt.st > rt.et {
		return ts, nil
	}
	mts, err := rt.gls.RangeReady(rt.st, rt.et, rt.limit)
	if err != nil {
		return nil, errors.WithMessage(err, "reload task error")
	}
	ts = make([]task.Task, 0, len(mts))
	for _, mt := range mts {
		rt.st = mt.Score + 1
		ts = append(ts, rt.c.Convert(mt))
		model.ReleaseTask(mt)
	}
	if len(ts) == 0 {
		rt.st = rt.et + 1
	}
	return ts, nil
}

func (rt *generateLoseTask) Next() int64 {
	return rt.st
}

func (rt *generateLoseTask) Valid() bool {
	return rt.st <= rt.et
}

type generatePool struct {
	gls GenerateLoseStore
	c   PbConvertTask
	ap  *sync.Pool
}

func NewGeneratePool(gls GenerateLoseStore, c PbConvertTask) *generatePool {
	return &generatePool{
		gls: gls,
		c:   c,
		ap: &sync.Pool{New: func() interface{} {
			return NewGenerateLoseTask()
		}},
	}
}

func (gp *generatePool) Generate(st, et, limit int64) GenerateLoseTask {
	rt := gp.ap.Get().(*generateLoseTask)
	rt.init(
		GenerateLoseTaskOptionWithGenerateLoseStore(gp.gls),
		GenerateLoseTaskOptionWithPbConvertTask(gp.c),
		GenerateLoseTaskOptionWithSt(st),
		GenerateLoseTaskOptionWithEt(et),
		GenerateLoseTaskOptionWithLimit(limit),
	)
	return rt
}

func (gp *generatePool) Release(glt GenerateLoseTask) {
	rt, ok := glt.(*generateLoseTask)
	if !ok {
		return
	}
	rt.reset()
	gp.ap.Put(rt)
}
