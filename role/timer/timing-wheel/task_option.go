package timing_wheel

import (
	"time"
)

type jobOption func(*job)

func jobOptionWithPos(pos int) jobOption {
	return func(j *job) {
		j.pos = pos
	}
}

func jobOptionWithCircle(circle int) jobOption {
	return func(j *job) {
		j.circle = circle
	}
}

func jobOptionWithScale(scale time.Duration) jobOption {
	return func(j *job) {
		j.scaleBase = scale
	}
}

func jobOptionWithScaleLevel(level time.Duration) jobOption {
	return func(j *job) {
		j.scaleLevel = level
	}
}
