package mongo

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/protocol/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type store struct {
	c *config

	client     *mongo.Client
	db         *mongo.Database
	collection *mongo.Collection
	ap         *sync.Pool // note 应该store公用，但目前存储一定是独立，先这样放着
}

func NewStore(client *mongo.Client, opts ...ConfigOption) *store {
	s := &store{
		c:      NewConfig(opts...),
		client: client,
		ap: &sync.Pool{New: func() interface{} {
			return make([]interface{}, 0, 20) // 先给20字段长度
		}},
	}
	s.db = s.client.Database(s.c.db, options.Database().SetReadPreference(readpref.SecondaryPreferred()))
	s.collection = s.db.Collection(s.c.collection)
	return s
}

func (s *store) Insert(t *model.Task) error {
	// note 后续这里要记录下主键ID
	_, err := s.collection.InsertOne(context.Background(), t)
	return errors.WithMessage(err, "Mongo数据写入失败[Insert]")
}

func (s *store) Retrieve(uid string) (*model.Task, error) {
	r := s.collection.FindOne(context.Background(), bson.M{"uid": uid})
	err := r.Err()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, xerror.WithXCodeMessage(xcode.DBRecordNotFound, "Mongo任务不存在")
		}
		return nil, errors.WithMessage(err, "Mongo数据获取失败[Retrieve]")
	}
	t := model.GenerateTask()
	err = r.Decode(t)
	if err != nil {
		model.ReleaseTask(t)
		return nil, errors.WithMessage(err, "Mongo数据协议转换失败[Retrieve]")
	}
	return t, nil
}

func (s *store) Remove(uid string) error {
	_, err := s.collection.DeleteOne(context.Background(), bson.M{"uid": uid})
	return errors.WithMessage(err, "Mongo数据删除失败")
}

func (s *store) Batch(uids []string) (ts []*model.Task, err error) {
	ctx := context.Background()
	c, ferr := s.collection.Find(ctx, bson.M{"uid": bson.M{"$in": uids}})
	if ferr != nil {
		err = errors.WithMessage(ferr, "Mongo数据获取失败[Batch]")
		return
	}
	ts = make([]*model.Task, 0, len(uids))
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
			err = errors.WithMessage(err, "Mongo数据协议转换失败[Batch]")
			return
		}
	}
	return
}

func (s *store) InsertMany(ts []*model.Task) error {
	args := s.ap.Get().([]interface{})
	if cap(args) < len(ts) {
		s.ap.Put(args)
		args = make([]interface{}, 0, len(ts))
	}
	defer func() {
		args = args[:0]
		s.ap.Put(args)
	}()
	for _, t := range ts {
		args = append(args, t)
	}
	_, err := s.collection.InsertMany(context.Background(), args)
	return errors.WithMessage(err, "Mongo数据写入失败[InsertMany]")
}

func (s *store) RemoveMany(uids []string) error {
	_, err := s.collection.DeleteMany(context.Background(), bson.M{"uid": bson.M{"$in": uids}})
	return errors.WithMessage(err, "Mongo数据批量删除失败[RemoveMany]")
}
