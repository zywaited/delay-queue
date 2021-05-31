package mongo

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/protocol/pb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type status struct {
	*store
}

func NewStatusStore(client *mongo.Client, opts ...ConfigOption) *status {
	return &status{store: NewStore(client, opts...)}
}

func (s *status) Status(uid string, tt pb.TaskType) error {
	_, err := s.store.collection.UpdateOne(
		context.Background(),
		bson.M{"uid": uid}, bson.M{"$set": bson.M{"type": int32(tt)}},
	)
	return errors.WithMessage(err, "Mongo数据更新类型状态失败")
}

func (s *status) NextTime(uid string, nt *time.Time) error {
	_, err := s.store.collection.UpdateOne(
		context.Background(),
		bson.M{"uid": uid}, bson.M{"$set": bson.M{"next_exec_time": nt.UnixNano()}},
	)
	return errors.WithMessage(err, "Mongo数据更新下次执行时间失败")
}

func (s *status) IncrRetryTimes(uid string, num int) error {
	_, err := s.store.collection.UpdateOne(
		context.Background(),
		bson.M{"uid": uid}, bson.M{"$inc": bson.M{"retry_times": num}},
	)
	return errors.WithMessage(err, "Mongo数据更新重试次数失败")
}

func (s *status) IncrSendTimes(uid string, num int) error {
	_, err := s.store.collection.UpdateOne(
		context.Background(),
		bson.M{"uid": uid}, bson.M{"$inc": bson.M{"times": num}},
	)
	return errors.WithMessage(err, "Mongo数据更新发送次数失败")
}
