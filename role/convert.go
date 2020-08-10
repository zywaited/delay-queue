package role

import (
	"time"

	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role/task"
)

type PbConvertTask interface {
	Convert(*model.Task) task.Task
}

type defaultPbConvertTask struct {
	fc task.Factory
}

func NewDefaultPbConvertTask(fc task.Factory) *defaultPbConvertTask {
	return &defaultPbConvertTask{fc: fc}
}

func (dt *defaultPbConvertTask) Convert(m *model.Task) task.Task {
	return dt.fc(
		task.ParamWithTime(task.Time{
			TTime: time.Duration(m.ExecTime),
			TType: task.AbsoluteTime,
		}),
		task.ParamWithUid(m.Uid),
		task.ParamWithName(m.Name),
		task.ParamWithRunner(nil),
		task.ParamWithType(pb.TaskType(m.Type)),
	)
}
