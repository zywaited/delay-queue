package task

import "github.com/zywaited/delay-queue/protocol/pb"

type (
	param struct {
		name string
		uid  string
		time Time
		run  Runner
		tt   pb.TaskType
	}

	ParamField func(*param)
)

func ParamWithName(name string) ParamField {
	return func(param *param) {
		param.name = name
	}
}

func ParamWithUid(uid string) ParamField {
	return func(param *param) {
		param.uid = uid
	}
}

func ParamWithTime(t Time) ParamField {
	return func(param *param) {
		param.time = t
	}
}

func ParamWithRunner(run Runner) ParamField {
	return func(param *param) {
		param.run = run
	}
}

func ParamWithType(tt pb.TaskType) ParamField {
	return func(param *param) {
		param.tt = tt
	}
}

func NewParam() *param {
	return &param{}
}

func (p *param) Name() string {
	return p.name
}

func (p *param) Uid() string {
	return p.uid
}

func (p *param) T() Time {
	return p.time
}

func (p *param) ACRunner() Runner {
	return p.run
}

func (p *param) Type() pb.TaskType {
	return p.tt
}
