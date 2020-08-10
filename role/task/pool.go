package task

import "sync"

type (
	Pool interface {
		NewTask() Task
		ReleaseTask(Task)
	}

	PoolTask interface {
		Bind(Pool)
	}

	newTask func() Task

	PFType      string
	PoolFactory interface {
		Name() string
		PFType() PFType

		NewPool(newTask) Pool
	}
)

const DefaultTaskPoolFactory PFType = "default-task-pool-factory"

var poolFactories map[PFType]PoolFactory

func init() {
	poolFactories = make(map[PFType]PoolFactory)
	RegisterPoolFactory(DefaultTaskPoolFactory, NewTaskPoolFactory())
	taskPoolFactories = make(map[PFType]Factory)
	RegisterTaskPoolFactory(DefaultTaskPoolFactory, NewDefaultFactory(AcPoolFactory(DefaultTaskPoolFactory)).Factory)
}

func RegisterPoolFactory(t PFType, factory PoolFactory) {
	poolFactories[t] = factory
}

func AcPoolFactory(t PFType) PoolFactory {
	if poolFactories[t] != nil {
		return poolFactories[t]
	}
	if poolFactories[DefaultTaskPoolFactory] != nil {
		return poolFactories[DefaultTaskPoolFactory]
	}
	for _, factory := range poolFactories {
		return factory
	}
	return nil
}

type taskPool struct {
	p *sync.Pool
}

func newTaskPool(nt newTask) *taskPool {
	return &taskPool{p: &sync.Pool{New: func() interface{} {
		return nt()
	}}}
}

func (tp *taskPool) NewTask() Task {
	tk := tp.p.Get().(Task)
	if pt, ok := tk.(PoolTask); ok {
		pt.Bind(tp)
	}
	return tk
}

func (tp *taskPool) ReleaseTask(tk Task) {
	tp.p.Put(tk)
}

type taskPoolFactory struct {
	n string
	t PFType
}

func NewTaskPoolFactory() *taskPoolFactory {
	return &taskPoolFactory{
		n: string(DefaultTaskPoolFactory),
		t: DefaultTaskPoolFactory,
	}
}

func (factory *taskPoolFactory) Name() string {
	return factory.n
}

func (factory *taskPoolFactory) PFType() PFType {
	return factory.t
}

func (factory *taskPoolFactory) NewPool(nt newTask) Pool {
	return newTaskPool(nt)
}

// 单独针对原型TASK的池化
var taskPoolFactories map[PFType]Factory

func RegisterTaskPoolFactory(t PFType, factory Factory) {
	taskPoolFactories[t] = factory
}

func AcTaskPoolFactory(t PFType) Factory {
	if taskPoolFactories[t] != nil {
		return taskPoolFactories[t]
	}
	if taskPoolFactories[DefaultTaskPoolFactory] != nil {
		return taskPoolFactories[DefaultTaskPoolFactory]
	}
	for _, factory := range taskPoolFactories {
		return factory
	}
	return nil
}

type Factory func(...ParamField) Task

type defaultFactory struct {
	p Pool
}

func NewDefaultFactory(pf PoolFactory) *defaultFactory {
	df := &defaultFactory{}
	df.p = pf.NewPool(func() Task {
		return NewBindTask()
	})
	return df
}

func (df *defaultFactory) Factory(opts ...ParamField) Task {
	t := df.p.NewTask().(*bindTask)
	t.init(opts...)
	return t
}
