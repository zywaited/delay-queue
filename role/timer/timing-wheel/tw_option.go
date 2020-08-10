package timing_wheel

import (
	"time"

	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/role/task"
)

type config struct {
	scale            time.Duration // 方便内部转换
	configScale      time.Duration
	configScaleLevel time.Duration     // 级别(秒级、毫秒级等)
	slotNum          int               // 槽数量
	newTaskStore     task.NewTaskStore // 任务存储
	maxLevel         int               // 最大层级
	currentLevel     int               // 当前层级
	logger           system.Logger
}

type Option func(*config)

func OptionWithSlotNum(slotNum int) Option {
	return func(c *config) {
		c.slotNum = slotNum
	}
}

func OptionWithNewTaskStore(newTaskStore task.NewTaskStore) Option {
	return func(c *config) {
		c.newTaskStore = newTaskStore
	}
}

func OptionWithMaxLevel(maxLevel int) Option {
	return func(c *config) {
		c.maxLevel = maxLevel
	}
}

func OptionWithLogger(logger system.Logger) Option {
	return func(c *config) {
		c.logger = logger
	}
}

func OptionWithConfigScale(configScale time.Duration) Option {
	return func(c *config) {
		c.configScale = configScale
	}
}

func OptionWithScaleLevel(level time.Duration) Option {
	return func(c *config) {
		if level > 0 {
			c.configScaleLevel = level
		}
	}
}

// 时间轮内部调用，用于标识当前层级
func optionWithLevel(level int) Option {
	return func(c *config) {
		c.currentLevel = level
	}
}

// 为了方便内部转换
func optionWithScale(scale time.Duration) Option {
	return func(c *config) {
		c.scale = scale
	}
}

func NewConfig() *config {
	return &config{configScaleLevel: 1}
}

func (c *config) clone() *config {
	return &config{
		scale:            c.scale,
		configScale:      c.configScale,
		configScaleLevel: c.configScaleLevel,
		slotNum:          c.slotNum,
		newTaskStore:     c.newTaskStore,
		maxLevel:         c.maxLevel,
		currentLevel:     c.currentLevel,
		logger:           c.logger,
	}
}
