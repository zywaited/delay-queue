package model

import (
	"time"

	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/go-common/xcopy"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Task struct {
	Id string `json:"id,omitempty"`

	Score        int64  `json:"score"`
	Uid          string `json:"uid"`
	Name         string `json:"name"`
	Args         string `json:"args"`
	Type         int32  `json:"type"`
	Times        int    `json:"times"`                    // 发送次数
	RetryTimes   int    `json:"retry_times"`              // 重试次数
	ExecTime     int64  `json:"exec_time"`                // 执行时间
	NextExecTime int64  `json:"next_exec_time,omitempty"` // 下次执行时间

	Schema  string `json:"schema,omitempty"`
	Address string `json:"address,omitempty"`
	Path    string `json:"path,omitempty"`

	CreatedAt int64 `json:"created_at,omitempty"`
	UpdatedAt int64 `json:"updated_at,omitempty"`
	DeletedAt int64 `json:"deleted_at,omitempty"`
}

func (t *Task) NextDelayTime() time.Duration {
	rt := 1
	if t.RetryTimes > 0 {
		rt = t.RetryTimes
	}
	// 1-3: 15 30 60 (s)
	// 4-6: 15 30 60 (m)
	// 7-9: 4 8 12 (h)
	// 10-~: 24 h
	if rt <= 3 {
		return (time.Second << (rt - 1)) * 15
	}
	if rt <= 6 {
		return (time.Minute << (rt - 4)) * 15
	}
	if rt <= 9 {
		return time.Hour * 4 * time.Duration(rt-6)
	}
	return time.Hour * 24
}

type DbTask struct {
	Id uint `json:"id"`

	Token        string      `json:"token"`
	Uid          string      `json:"uid"`
	Score        int64       `json:"score"`
	Name         string      `json:"name"`
	Type         pb.TaskType `json:"type"`
	Times        int         `json:"times"`          // 发送次数
	RetryTimes   int         `json:"retry_times"`    // 重试次数
	ExecTime     time.Time   `json:"exec_time"`      // 执行时间
	NextExecTime *time.Time  `json:"next_exec_time"` // 下次执行时间

	Schema  string `json:"schema"`
	Address string `json:"address"`
	Path    string `json:"path"`

	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at"`
}

func (t *DbTask) TableName() string {
	return "delay_queue.task"
}

type RedisTask map[string]interface{}

func (t *Task) ConvertRedisTask() RedisTask {
	return xcopy.ToMap(t)
}

func (rt RedisTask) Args(args *[]interface{}) {
	for field, value := range rt {
		*args = append(*args, field)
		if tt, ok := value.(time.Duration); ok {
			*args = append(*args, int64(tt))
			continue
		}
		if pt, ok := value.(pb.TaskType); ok {
			*args = append(*args, int32(pt))
			continue
		}
		*args = append(*args, value)
	}
}

type MongoTask struct {
	Id primitive.ObjectID `json:"id,omitempty" bson:"_id,omitempty" copy:"-"`

	Score        int64  `json:"score" bson:"score"`
	Token        string `json:"token" bson:"token"`
	Uid          string `json:"uid" bson:"uid"`
	Name         string `json:"name" bson:"name"`
	Args         string `json:"args" bson:"args"`
	Type         int32  `json:"type" bson:"type"`
	Times        int    `json:"times" bson:"times"`                             // 发送次数
	RetryTimes   int    `json:"retry_times" bson:"retry_times"`                 // 重试次数
	ExecTime     int64  `json:"exec_time" bson:"exec_time"`                     // 执行时间
	NextExecTime int64  `json:"next_exec_time,omitempty" bson:"next_exec_time"` // 下次执行时间

	Schema  string `json:"schema,omitempty" bson:"schema"`
	Address string `json:"address,omitempty" bson:"address"`
	Path    string `json:"path,omitempty" bson:"path"`

	CreatedAt int64 `json:"created_at,omitempty" bson:"created_at"`
	UpdatedAt int64 `json:"updated_at,omitempty" bson:"updated_at"`
	DeletedAt int64 `json:"deleted_at,omitempty" bson:"deleted_at"`
}

func (t *Task) ConvertMongoTask(cp xcopy.XCopy, mt *MongoTask) {
	_ = cp.CopySF(mt, t)
	mt.Id = primitive.NilObjectID
}

func (mt *MongoTask) ConvertTask(cp xcopy.XCopy, t *Task) {
	_ = cp.CopySF(t, mt)
}

func (mt *MongoTask) Collection() string {
	return "task"
}
