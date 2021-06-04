package limiter

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
)

const defaultCheckTime = time.Minute * 5

var (
	ErrTimeout    = errors.New("pool handle timeout")
	ErrNotRunning = errors.New("pool handle not running")
)

type TaskJob func()

type (
	Pool interface {
		role.Launcher
		role.LauncherStatus
		Submit(context.Context, TaskJob) error
	}

	WorkerGroup interface {
		Len() int
		Add(Worker)
		AcquireOne() Worker
		Acquire(int) []Worker
		Expire(time.Duration, int) []Worker
	}
)

type (
	PoolOptions   func(*pool)
	WorkerCreator func() Worker
)

func PoolOptionsWithLimit(limit int32) PoolOptions {
	return func(p *pool) {
		p.limit = limit
	}
}

func PoolOptionsWithIdle(idle int) PoolOptions {
	return func(p *pool) {
		p.idle = idle
	}
}

func PoolOptionsWithIdleTime(t time.Duration) PoolOptions {
	return func(p *pool) {
		if t > 0 {
			p.idleTime = t
		}
	}
}

func PoolOptionsWithCheckNum(num int) PoolOptions {
	return func(p *pool) {
		p.checkNum = num
	}
}

func PoolOptionsWithLogger(logger system.Logger) PoolOptions {
	return func(p *pool) {
		p.logger = logger
	}
}

func PoolOptionsWithWorkerCreator(wc WorkerCreator) PoolOptions {
	return func(p *pool) {
		p.wc = wc
	}
}

type pool struct {
	ap       *sync.Pool
	wc       WorkerCreator
	ws       WorkerGroup
	logger   system.Logger
	status   int32
	running  int32
	limit    int32
	checkNum int
	idle     int
	wt       chan TaskJob
	wr       chan Worker
	idleTime time.Duration
}

func NewPool(ws WorkerGroup, opts ...PoolOptions) *pool {
	limit := runtime.GOMAXPROCS(0) * 100
	p := &pool{
		ws:       ws,
		limit:    int32(limit),
		checkNum: limit,
		idle:     int(math.Ceil(float64(limit) * 0.25)),
		idleTime: defaultCheckTime,
		wt:       make(chan TaskJob, 128),
		wr:       make(chan Worker, 128),
		status:   role.StatusInitialized,
	}
	p.ap = &sync.Pool{New: func() interface{} {
		return newPoolWorker(p.wc())
	}}
	for _, opt := range opts {
		opt(p)
	}
	if p.idle == 0 || p.idle > int(p.limit) {
		p.idle = int(p.limit)
	}
	if p.wc == nil {
		p.wc = defaultWorkerCreator
	}
	return p
}

func (p *pool) Run() error {
	for {
		cv := atomic.LoadInt32(&p.status)
		if cv == role.StatusRunning {
			return nil
		}
		if !atomic.CompareAndSwapInt32(&p.status, cv, role.StatusRunning) {
			continue
		}
		if cv == role.StatusForceSTW || cv == role.StatusInitialized {
			go p.run()
		}
		return nil
	}
}

func (p *pool) Stop(st role.StopType) error {
	for {
		cv := atomic.LoadInt32(&p.status)
		if cv == role.StatusGraceFulST {
			return nil
		}
		if !atomic.CompareAndSwapInt32(&p.status, cv, role.StatusGraceFulST) {
			continue
		}
		return nil
	}
}

func (p *pool) Submit(ctx context.Context, tj TaskJob) error {
	if p.Stopped() {
		return ErrNotRunning
	}
	select {
	case p.wt <- tj:
	case <-ctx.Done():
		return ErrTimeout
	}
	return nil
}

func (p *pool) Running() bool {
	return atomic.LoadInt32(&p.status) == role.StatusRunning
}

func (p *pool) Stopped() bool {
	return !p.Running()
}

func (p *pool) run() {
	i := time.NewTicker(p.idleTime)
	defer func() {
		if err := recover(); err != nil && p.logger != nil {
			p.logger.Errorf("limiter-pool acquire panic: %v, stack: %s", err, system.Stack())
		}
		i.Stop()
	}()
	var (
		tj  TaskJob
		wc  chan<- TaskJob
		ewc chan<- TaskJob
		wt  = p.wt
		ic  = i.C
		ews workers
	)
	acquireWs := func() chan<- TaskJob {
		w := p.acquire()
		if w == nil {
			w = ews.acquire()
		}
		if w == nil {
			return nil
		}
		return w.(*poolWorker).tasks
	}
	acquireEws := func() chan<- TaskJob {
		w := ews.acquire()
		if w == nil {
			return nil
		}
		return w.(*poolWorker).tasks
	}
	acquireW := func() chan<- TaskJob {
		c := acquireWs()
		if c == nil && ewc != nil {
			c = ewc
			ewc = nil
		}
		return c
	}
	for {
		ic = i.C
		if tj != nil && wc == nil {
			wc = acquireW()
		}
		if ewc == nil {
			ewc = acquireEws()
		}
		if ews.len() > 0 {
			ic = nil
		}
		select {
		case <-ic:
			ews = p.expire()
		case j := <-wt:
			tj = j
			wt = nil
			wc = acquireWs()
		case wc <- tj:
			wc = nil
			tj = nil
			wt = p.wt
		case ewc <- nil:
			ewc = nil
		case w := <-p.wr:
			if tj != nil && wc == nil {
				wc = w.(*poolWorker).tasks
				break
			}
			p.ws.Add(w)
		}
	}
}

func (p *pool) expire() []Worker {
	checkNum := p.checkNum
	if checkNum <= 0 {
		checkNum = p.ws.Len()
	}
	ews := p.ws.Expire(p.idleTime, checkNum)
	if len(ews) > 0 {
		return ews
	}
	if p.idle > 0 && p.ws.Len() > p.idle {
		ews = p.ws.Acquire(p.ws.Len() - p.idle)
	}
	return ews
}

func (p *pool) acquire() Worker {
	w := p.ws.AcquireOne()
	if w == nil && (p.limit <= 0 || atomic.LoadInt32(&p.running) < p.limit) {
		w = p.newWorker()
	}
	return w
}

func (p *pool) newWorker() Worker {
	w := p.ap.Get().(*poolWorker)
	atomic.AddInt32(&p.running, 1)
	go func() {
		defer func() {
			if err := recover(); err != nil && p.logger != nil {
				p.logger.Errorf("limiter-pool-worker panic: %v, stack: %s", err, system.Stack())
			}
			p.ap.Put(w)
			atomic.AddInt32(&p.running, -1)
		}()
		for task := range w.tasks {
			if task == nil {
				return
			}
			w.Run(task)
			w.last = time.Now()
			p.wr <- w
		}
	}()
	return w
}
