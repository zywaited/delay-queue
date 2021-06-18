package mongo

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/save95/xerror"
	"github.com/save95/xerror/xcode"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/go-common/xcopy"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type store struct {
	c *config

	client     *mongo.Client
	db         *mongo.Database
	collection *mongo.Collection
	ap         *sync.Pool
}

func NewStore(client *mongo.Client, opts ...ConfigOption) *store {
	s := &store{
		c:      NewConfig(opts...),
		client: client,
	}
	s.ap = &sync.Pool{New: func() interface{} {
		return &model.MongoTask{Token: s.c.token}
	}}
	// clint init options.Database().SetReadPreference(readpref.SecondaryPreferred())
	s.db = s.client.Database(s.c.db)
	s.collection = s.db.Collection(s.c.collection)
	if s.c.cp == nil {
		s.c.cp = xcopy.NewCopy()
	}
	return s
}

func (s *store) Insert(t *model.Task) error {
	return s.insert(context.Background(), t)
}

func (s *store) insert(ctx context.Context, t *model.Task) error {
	// note 后续这里要记录下主键ID
	mt := s.ap.Get().(*model.MongoTask)
	defer s.ap.Put(mt)
	t.ConvertMongoTask(s.c.cp, mt)
	_, err := s.collection.InsertOne(ctx, mt)
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
	mt := s.ap.Get().(*model.MongoTask)
	defer s.ap.Put(mt)
	err = r.Decode(mt)
	if err != nil {
		return nil, errors.WithMessage(err, "Mongo数据协议转换失败[Retrieve]")
	}
	t := model.GenerateTask()
	mt.ConvertTask(s.c.cp, t)
	return t, nil
}

func (s *store) Remove(uid string) error {
	return s.remove(context.Background(), uid)
}

func (s *store) remove(ctx context.Context, uid string) error {
	_, err := s.collection.DeleteOne(ctx, bson.M{"uid": uid})
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
	mt := s.ap.Get().(*model.MongoTask)
	defer func() {
		_ = c.Close(ctx)
		s.ap.Put(mt)
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
		err = c.Decode(mt)
		if err != nil {
			err = errors.WithMessage(err, "Mongo数据协议转换失败[Batch]")
			return
		}
		t := model.GenerateTask()
		mt.ConvertTask(s.c.cp, t)
		ts = append(ts, t)
	}
	return
}

func (s *store) InsertMany(ts []*model.Task) error {
	return s.insertMany(context.Background(), ts)
}

func (s *store) insertMany(ctx context.Context, ts []*model.Task) error {
	records := make([]interface{}, 0, len(ts))
	defer func() {
		for _, record := range records {
			s.ap.Put(record)
		}
	}()
	for _, t := range ts {
		mt := s.ap.Get().(*model.MongoTask)
		t.ConvertMongoTask(s.c.cp, mt)
		records = append(records, mt)
	}
	_, err := s.collection.InsertMany(ctx, records)
	return errors.WithMessage(err, "Mongo数据写入失败[InsertMany]")
}

func (s *store) RemoveMany(uids []string) error {
	return s.removeMany(context.Background(), uids)
}

func (s *store) removeMany(ctx context.Context, uids []string) error {
	_, err := s.collection.DeleteMany(ctx, bson.M{"uid": bson.M{"$in": uids}})
	return errors.WithMessage(err, "Mongo数据批量删除失败[RemoveMany]")
}
