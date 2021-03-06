package redis

import (
	"time"

	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/task"
)

// 只有重启时候才会调用
type ReloadTask struct {
	gls role.GenerateLoseStore

	et int64
	c  role.PbConvertTask
}

func NewReload(gls role.GenerateLoseStore, c role.PbConvertTask) *ReloadTask {
	return &ReloadTask{
		gls: gls,
		et:  time.Now().UnixNano(),
		c:   c,
	}
}

func (r *ReloadTask) Reload(offset, limit int64) ([]task.Task, error) {
	mts, err := r.gls.RangeReady(0, r.et, offset, limit)
	if err != nil {
		return nil, errors.WithMessage(err, "reload task error")
	}
	ts := make([]task.Task, 0, len(mts))
	for _, mt := range mts {
		ts = append(ts, r.c.Convert(mt))
		model.ReleaseTask(mt)
	}
	return ts, nil
}

func (r *ReloadTask) Len() (int, error) {
	l, err := r.gls.ReadyNum(0, r.et)
	return l, errors.WithMessage(err, "reload task error")
}
