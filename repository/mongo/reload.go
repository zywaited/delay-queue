package mongo

import (
	"context"

	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
)

type generateLoseStore struct {
	c *config

	collection *mongo.Collection
}

func NewGenerateLoseStore(client *mongo.Client, opts ...ConfigOption) *generateLoseStore {
	gl := &generateLoseStore{c: NewConfig(opts...)}
	gl.collection = client.Database(gl.c.db).Collection(gl.c.collection)
	return gl
}

func (gl *generateLoseStore) RangeReady(st, et, limit int64) (ts []*model.Task, err error) {
	// st <= t <= et
	ctx := context.Background()
	c, ferr := gl.collection.Find(
		ctx,
		bson.M{"created_at": bson.M{"$lte": st, "$gte": et}, "type": bson.M{"$not": pb.TaskType_TaskFinished}},
		options.Find().SetLimit(limit),
	)
	if ferr != nil {
		err = errors.WithMessage(ferr, "Mongo数据获取失败[RangeReady]")
		return
	}
	ts = make([]*model.Task, 0)
	defer func() {
		_ = c.Close(ctx)
		if err == nil {
			return
		}
		// replace
		for _, t := range ts {
			model.ReleaseTask(t)
		}
		ts = nil
	}()
	// 这里不用c.All，控制内存
	for c.Next(ctx) {
		t := model.GenerateTask()
		ts = append(ts, t)
		err = c.Decode(t)
		if err != nil {
			err = errors.WithMessage(err, "Mongo数据协议转换失败[RangeReady]")
			return
		}
	}
	return
}

func (gl *generateLoseStore) ReadyNum(st, et int64) (n int64, err error) {
	ctx := context.Background()
	n, err = gl.collection.CountDocuments(
		ctx,
		bson.M{"created_at": bson.M{"$lte": st, "$gte": et}, "type": bson.M{"$not": pb.TaskType_TaskFinished}},
	)
	if err != nil {
		err = errors.WithMessage(err, "Mongo数据获取失败[RangeReady]")
	}
	return
}

func (gl *generateLoseStore) NextReady(st, et, limit int64) (int64, error) {
	ctx := context.Background()
	r := gl.collection.FindOne(
		ctx,
		bson.M{"created_at": bson.M{"$lte": st, "$gte": et}, "type": bson.M{"$not": pb.TaskType_TaskFinished}},
		options.FindOne().SetSort(bson.D{{"created_at", 1}, {"_id", 1}}),
		options.FindOne().SetSkip(limit-1),
		options.FindOne().SetProjection(bson.D{{"_id", 1}, {"uid", 1}, {"created_at", 1}}),
	)
	bs, err := r.DecodeBytes()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return et, nil
		}
		return 0, errors.WithMessage(err, "Mongo数据获取失败[NextReady]")
	}
	max, err := bs.LookupErr("created_at")
	if err != nil {
		if err == bsoncore.ErrElementNotFound {
			return et, nil
		}
		return 0, errors.WithMessage(err, "Mongo数据获取失败[NextReady]")
	}
	return max.Int64(), nil
}
