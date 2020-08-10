package runner

import "github.com/zywaited/delay-queue/role/task"

type Type string

const DefaultRunnerName Type = "default-runner"

var runners map[Type]task.Runner

func init() {
	runners = make(map[Type]task.Runner)
}

func RegisterRunner(t Type, rn task.Runner) {
	runners[t] = rn
}

func AcRunner(t Type) task.Runner {
	if len(runners) == 0 {
		return nil
	}
	if runners[t] != nil {
		return runners[t]
	}
	if runners[DefaultRunnerName] != nil {
		return runners[DefaultRunnerName]
	}
	for _, rn := range runners {
		return rn
	}
	return nil
}
