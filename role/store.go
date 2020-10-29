package role

import (
	"time"

	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role/task"
)

type DataStoreType string

// 存储结构
type DataStore interface {
	Insert(*model.Task) error

	Retrieve(string) (*model.Task, error)

	Batch([]string) ([]*model.Task, error)

	InsertMany([]*model.Task) error

	Remove(string) error

	RemoveMany([]string) error
}

type DataStoreUpdater interface {
	Status(string, pb.TaskType) error
	NextTime(string, *time.Time) error
	IncrRetryTimes(string, int) error
	IncrSendTimes(string, int) error
}

// 恢复数据时使用
// 也就时间轮重启会处理该逻辑
type GenerateLoseTask interface {
	Len() (int, error)
	Reload(int64, int64) ([]task.Task, error)
}

type GenerateLoseStore interface {
	RangeReady(int64, int64, int64, int64) ([]*model.Task, error)
	ReadyNum(int64, int64) (int, error)
}

type ReloadStore interface {
	Start()
	End()
	Len() int
	Pop() (string, error)
}

type DataSourceTransaction interface {
	Transaction(func(DataStore) error) error
}
