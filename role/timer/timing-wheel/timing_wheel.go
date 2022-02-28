package timing_wheel

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"time"

	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
)

// TimeWheel 时间轮
// interval暂不考虑层级过高导致的溢出(限制最大层级)
type TimeWheel struct {
	c                 *config
	ticker            *ticker
	scale             time.Duration  // 指针每隔多久往前移动一格
	now               time.Duration  // 当前时间
	interval          time.Duration  // 一圈所需时间
	slots             []task.Store   // 时间轮槽
	taskIndex         map[string]int // 任务全局索引(为了不循环每一个卡槽)
	currentPos        int            // 当前指针
	addTaskChannel    chan Task      // 新增任务channel
	removeTaskChannel chan Task      // 删除任务channel
	stopChannel       chan bool      // 停止定时器channel
	nextLevel         *TimeWheel     // 下一层时间轮
	baseLevel         *TimeWheel     // 底层时间轮
	usedLevel         int
	stopped           int32
}

func NewTimeWheel(opts ...Option) *TimeWheel {
	c := NewConfig()
	for _, opt := range opts {
		opt(c)
	}
	optionWithScale(realScaleOption(c.configScale, c.configScaleLevel))(c)
	return newTimeWheelWithConfig(c)
}

func newTimeWheelWithConfig(c *config) *TimeWheel {
	if c.scale <= 0 || c.slotNum <= 0 || c.newTaskStore == nil {
		return nil
	}
	optionWithLevel(c.currentLevel + 1)(c) // 层级+1
	tw := &TimeWheel{
		c:                 c,
		scale:             c.scale,
		interval:          c.scale * time.Duration(c.slotNum),
		slots:             make([]task.Store, c.slotNum),
		taskIndex:         make(map[string]int),
		currentPos:        0,
		addTaskChannel:    make(chan Task, 1),
		removeTaskChannel: make(chan Task, 1),
		stopChannel:       make(chan bool),
		stopped:           role.StatusInitialized,
	}
	tw.initSlots()
	// 先设置成自身
	tw.setBaseLevel(tw)
	tw.usedLevel = c.currentLevel
	return tw
}

func (tw *TimeWheel) running() bool {
	return atomic.LoadInt32(&tw.stopped) == role.StatusRunning
}

func (tw *TimeWheel) setBaseLevel(base *TimeWheel) {
	tw.baseLevel = base
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.c.slotNum; i++ {
		tw.slots[i] = tw.c.newTaskStore(tw.c.currentLevel, i)
	}
}

func (tw *TimeWheel) initNextLevel() *TimeWheel {
	if tw.c.currentLevel < tw.c.maxLevel && tw.nextLevel == nil {
		cc := tw.c.clone()
		optionWithScale(tw.interval)(cc) // 重置刻度
		ntw := newTimeWheelWithConfig(cc)
		ntw.setBaseLevel(tw.baseLevel)
		ntw.baseLevel.usedLevel = ntw.usedLevel
		go ntw.run()
		// 这里需要等待启动(只是稍作停留: 1/4)
		for !ntw.running() {
			time.Sleep(tw.baseLevel.c.configScale >> 2)
		}
		tw.nextLevel = ntw
	}
	return tw.nextLevel
}

func (tw *TimeWheel) initTicker() *ticker {
	tw.ticker = newTicker(tickerOptionWithSt(tw == tw.baseLevel), tickerOptionWithTime(tw.c.configScale))
	return tw.ticker
}

func (tw *TimeWheel) getPosAndCircle(exec time.Duration) (pos, circle int) {
	dt := exec - time.Duration(tw.currentPos)*tw.scale
	pos = (tw.currentPos + int(dt/tw.scale)) % tw.c.slotNum
	circle = int(math.Ceil(math.Ceil(float64(exec)/float64(tw.scale)) / float64(tw.c.slotNum)))
	return
}

func (tw *TimeWheel) run() {
	if tw.nextLevel != nil {
		// 防止下一层中断没有启动
		// 因为不为空所以启动过
		go tw.nextLevel.run()
	}
	if tw.running() {
		return
	}
	atomic.StoreInt32(&tw.stopped, role.StatusRunning)
	tr := tw.initTicker()
	tw.now = time.Duration(time.Now().UnixNano())
	defer func() {
		if err := recover(); err != nil && tw.c.logger != nil {
			tw.c.logger.Errorf("timing-wheel[%d] panic: %v, stack: %s", tw.c.currentLevel, err, system.Stack())
			atomic.StoreInt32(&tw.stopped, role.StatusForceSTW)
		}
		if tr != nil {
			tr.stop()
		}
		if tw.c.logger != nil {
			tw.c.logger.Infof("timing-wheel[%d] stop", tw.c.currentLevel)
		}
	}()
	if tw.c.logger != nil {
		tw.c.logger.Infof("timing-wheel[%d] run", tw.c.currentLevel)
	}
	// 推动通道
	tc := tr.ct()
	for {
		select {
		case <-tc:
			tw.tickUp()
		case t := <-tw.addTaskChannel:
			tw.addTask(t)
		case t := <-tw.removeTaskChannel:
			tw.removeTask(t)
		case <-tw.stopChannel:
			if tw.nextLevel != nil {
				tw.nextLevel.Stop()
			}
			atomic.StoreInt32(&tw.stopped, role.StatusForceSTW)
			return
		}
	}
}

func (tw *TimeWheel) addTask(t Task) {
	var err error
	pt, ok := t.(task.Result)
	defer func() {
		if rerr := recover(); rerr != nil {
			if tw.c.logger != nil {
				tw.c.logger.Errorf(
					"task: %s add error: timing-wheel[%d]: %v, stack: %s",
					t.Uid(), tw.c.currentLevel, rerr, system.Stack(),
				)
			}
			if ok {
				pt.CloseResult(errors.New("timing-wheel add task panic"))
			}
		}
	}()
	tt := time.Duration(tw.currentPos)*tw.scale + t.Remaining()
	if tt >= tw.interval {
		// 下一层
		tw.initNextLevel()
		if tw.nextLevel != nil {
			// 所有内部数据流转都走chanel，保证所有的逻辑一致
			_ = tw.c.gp.Submit(context.Background(), func() {
				tw.nextLevel.Add(t)
			})
			return
		}
		// 如果达到最大限制，先统一放到当前层中
	}
	defer func() {
		if ok {
			pt.CloseResult(err)
		}
	}()
	// 可在当前时间轮中
	pos, circle := tw.getPosAndCircle(tt)
	// 异步调度导致的时间误差
	if pos <= tw.currentPos {
		_ = tw.c.gp.Submit(context.Background(), func() {
			tw.baseLevel.Add(t)
		})
		return
	}
	t.index(pos, circle)
	if err = tw.slots[pos].Add(t); err != nil {
		if tw.c.logger != nil {
			tw.c.logger.Infof(
				"task: %s add error: timing-wheel[%d]: %s",
				t.Uid(), tw.c.currentLevel, err.Error(),
			)
		}
		return
	}
	// todo 时间轮不再删除数据
	//tw.taskIndex[t.Uid()] = pos
	if tw.c.logger != nil {
		tw.c.logger.Infof(
			"task[%s]: %s add to timing-wheel, level: %d, pos: [%d-%d], circle: %d",
			t.Uid(),
			t.Type(),
			tw.c.currentLevel,
			tw.currentPos, pos, circle,
		)
	}
}

func (tw *TimeWheel) removeTask(t Task) {
	pt, ok := t.(task.Result)
	defer func() {
		if rerr := recover(); rerr != nil {
			if tw.c.logger != nil {
				tw.c.logger.Errorf(
					"task: %s remove error: timing-wheel[%d]: %v, stack: %s",
					t.Uid(), tw.c.currentLevel, rerr, system.Stack(),
				)
			}
			if ok {
				pt.CloseResult(errors.New("timing-wheel remove task panic"))
			}
		}
	}()
	storeIndex, sok := tw.taskIndex[t.Uid()]
	if sok {
		var err error
		defer func() {
			if ok {
				pt.CloseResult(err)
			}
		}()
		if err = tw.slots[storeIndex].Remove(t.Uid()); err != nil {
			if tw.c.logger != nil {
				tw.c.logger.Infof(
					"task: %s remove error: timing-wheel[%d]: %s",
					t.Uid(), tw.c.currentLevel, err.Error(),
				)
			}
			return
		}
		delete(tw.taskIndex, t.Uid())
		return
	}
	if tw.nextLevel != nil {
		// 所有内部数据流转都走chanel，保证所有的逻辑一致
		_ = tw.c.gp.Submit(context.Background(), func() {
			tw.nextLevel.Remove(t)
		})
		return
	}
	// fix: 所有层级都不存在的话关闭即可
	if ok {
		pt.CloseResult(nil)
	}
}

func (tw *TimeWheel) nextTickUp() {
	if tw.nextLevel != nil && tw.nextLevel.ticker != nil {
		tw.nextLevel.ticker.up()
	}
}

func (tw *TimeWheel) tickUp() {
	defer func() {
		if err := recover(); err != nil && tw.c.logger != nil {
			tw.c.logger.Errorf("timing-wheel[%d] tick panic: %v, stack: %s", tw.c.currentLevel, err, system.Stack())
		}
	}()
	now := time.Duration(time.Now().UnixNano())
	// 时间补偿
	times := (now - tw.now) / tw.c.configScaleLevel / tw.scale
	if times == 0 {
		times = 1
	}
	for ; times >= 1; times-- {
		tw.tick()
	}
	tw.now = now
}

func (tw *TimeWheel) tick() {
	tw.currentPos++
	if tw.currentPos == tw.c.slotNum {
		// 进入下一层&当前层索引清0
		tw.currentPos = 0
		tw.nextTickUp()
		return
	}
	store := tw.slots[tw.currentPos]
	if store.Len() == 0 {
		return
	}
	if tw.c.logger != nil {
		tw.c.logger.Infof("timing-wheel[%d][%d]: run task", tw.c.currentLevel, tw.currentPos)
	}
	// 这里为了性能分开处理
	tw.slots[tw.currentPos] = tw.c.newTaskStore(tw.c.currentLevel, tw.currentPos)
	_ = tw.c.gp.Submit(context.Background(), func() {
		tw.scanTasks(store)
	})
}

func (tw *TimeWheel) scanTasks(store task.Store) {
	defer func() {
		if err := recover(); err != nil {
			tw.c.logger.Errorf("task exec error: %v, stack: %s", err, system.Stack())
		}
		store.Release()
	}()
	iter := store.Iter()
	defer iter.Release()
	for iter.Next() {
		t := iter.Scan().(Task)
		// 所有内部数据流转都走chanel，保证所有的逻辑一致(也为了不用加锁)
		if pt, ok := t.(task.Result); ok {
			// 因为走channel，异步所有要阻塞完成
			pt.InitResult()
			tw.Remove(t)
			_ = pt.WaitResult()
		}
		if tw != tw.baseLevel && !t.Ready() {
			// 重新入时间轮(底层)
			tw.c.logger.Infof("task[%s] not ready, retry add to timing-wheel", t.Uid())
			tw.baseLevel.Add(t)
			continue
		}
		tw.runTask(t)
	}
}

// 统一执行，防止内部崩溃
func (tw *TimeWheel) runTask(t Task) {
	_ = tw.c.gp.Submit(context.Background(), func() {
		defer func() {
			if err := recover(); err != nil {
				tw.c.logger.Errorf("task: %s exec error: %v, stack: %s", t.Uid(), err, system.Stack())
			}
			t.Release()
		}()
		if err := t.Run(); err != nil && tw.c.logger != nil {
			tw.c.logger.Infof("task: %s exec error: %s", t.Uid(), err.Error())
		}
	})
}

func (tw *TimeWheel) Remove(t Task) {
	if t.Uid() == "" {
		return
	}
	if !tw.running() {
		if tw.c.logger != nil {
			tw.c.logger.Infof(
				"task: %s remove error: timing-wheel[%d] stopped",
				t.Uid(), tw.c.currentLevel,
			)
		}
		return
	}
	tw.removeTaskChannel <- t
}

func (tw *TimeWheel) Add(t Task) {
	if t.Uid() == "" {
		return
	}
	// 最小刻度&配置级别, 用于判断任务是否到期(只有第一层才处理)
	// todo 由上层处理
	//if tw == tw.baseLevel {
	//	t.scale(tw.scale, tw.c.configScaleLevel)
	//}
	if t.Ready() {
		if tw.c.logger != nil {
			tw.c.logger.Infof("timing-wheel[%d], task[%s] add ready", tw.c.currentLevel, t.Uid())
		}
		// runTask会自动释放
		if pt, ok := t.(task.Result); ok {
			pt.CloseResult(nil)
		}
		tw.runTask(t)
		return
	}
	if !tw.running() {
		if tw.c.logger != nil {
			tw.c.logger.Infof(
				"task: %s add error: timing-wheel[%d] stopped",
				t.Uid(), tw.c.currentLevel,
			)
		}
		// 不进入队列自动释放
		if pt, ok := t.(task.Result); ok {
			pt.CloseResult(errors.New("timing-wheel stopped"))
		}
		t.Release()
		return
	}
	tw.addTaskChannel <- t
}

func (tw *TimeWheel) Run() {
	go tw.run()
}

func (tw *TimeWheel) Stop() {
	if tw.running() {
		tw.stopChannel <- true
	}
	return
}
