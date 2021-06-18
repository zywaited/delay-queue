package mongo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/zywaited/delay-queue/parser/system"
	"github.com/zywaited/delay-queue/protocol/model"
	"github.com/zywaited/delay-queue/protocol/pb"
	"github.com/zywaited/delay-queue/role"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// version >= 4.0
type transaction struct {
	*status
	ap *sync.Pool
}

func NewTransactionStore(client *mongo.Client, opts ...ConfigOption) *transaction {
	return &transaction{
		status: NewStatusStore(client, opts...),
		ap: &sync.Pool{New: func() interface{} {
			return &transactionReal{}
		}},
	}
}

func (t *transaction) Transaction(fn func(role.DataStore) error) (err error) {
	s, err := t.status.client.StartSession()
	if err != nil {
		return errors.WithMessage(err, "mongo transaction session")
	}
	defer s.EndSession(context.Background())
	err = s.StartTransaction()
	if err != nil {
		return errors.WithMessage(err, "mongo transaction start")
	}
	tr := t.ap.Get().(*transactionReal)
	tr.status = t.status
	tr.ctx = mongo.NewSessionContext(context.Background(), s)
	defer func() {
		if re := recover(); re != nil {
			err = fmt.Errorf("mongo transaction: %v, stack: %s", re, system.Stack())
		}
		if err == nil {
			err = s.CommitTransaction(context.Background())
		}
		if err != nil {
			err = s.AbortTransaction(context.Background())
		}
		err = errors.WithMessage(err, "mongo transaction commit")
		tr.ctx = nil
		t.ap.Put(tr)
	}()
	return errors.WithMessage(fn(tr), "mongo业务逻辑出错[Transaction]")
}

type transactionReal struct {
	*status
	ctx context.Context
}

// 主要实现修改相关操作
func (tr *transactionReal) Insert(t *model.Task) error {
	return tr.insert(tr.ctx, t)
}

func (tr *transactionReal) InsertMany(ts []*model.Task) error {
	return tr.insertMany(tr.ctx, ts)
}

func (tr *transactionReal) Remove(uid string) error {
	return tr.remove(tr.ctx, uid)
}

func (tr *transactionReal) RemoveMany(uids []string) error {
	return tr.removeMany(tr.ctx, uids)
}

func (tr *transactionReal) Status(uid string, tt pb.TaskType) error {
	return tr.status.status(tr.ctx, uid, tt)
}

func (tr *transactionReal) NextTime(uid string, nt *time.Time) error {
	return tr.nextTime(tr.ctx, uid, nt)
}

func (tr *transactionReal) IncrRetryTimes(uid string, num int) error {
	return tr.incrRetryTimes(tr.ctx, uid, num)
}

func (tr *transactionReal) IncrSendTimes(uid string, num int) error {
	return tr.incrSendTimes(tr.ctx, uid, num)
}

// version < 4.0 [单表不重试]
type lowerTransaction struct {
	*status
	ap *sync.Pool
}

func NewLowerTransactionStore(client *mongo.Client, opts ...ConfigOption) *lowerTransaction {
	return &lowerTransaction{
		status: NewStatusStore(client, opts...),
		ap: &sync.Pool{New: func() interface{} {
			return &lowerTransactionReal{}
		}},
	}
}

func (lt *lowerTransaction) Transaction(fn func(role.DataStore) error) (err error) {
	nt := lt.ap.Get().(*lowerTransactionReal)
	nt.status = lt.status
	defer func() {
		if re := recover(); re != nil {
			err = fmt.Errorf("mongo lower transaction: %v, stack: %s", re, system.Stack())
		}
		nt.status = nil
		for _, m := range nt.data {
			mt, ok := m.(*mongo.InsertOneModel)
			if ok {
				lt.status.ap.Put(mt.Document)
			}
		}
		nt.data = nt.data[:0]
		lt.ap.Put(nt)
	}()
	err = errors.WithMessage(fn(nt), "mongo业务逻辑出错[LowerTransaction]")
	if err != nil || len(nt.data) == 0 {
		return
	}
	_, err = lt.status.collection.BulkWrite(context.Background(), nt.data, options.BulkWrite().SetOrdered(false))
	if err != nil {
		err = errors.WithMessage(fn(nt), "mongo bulk write 逻辑出错[LowerTransaction]")
	}
	return
}

type lowerTransactionReal struct {
	*status
	data []mongo.WriteModel
}

// 主要实现修改相关操作
func (lt *lowerTransactionReal) Insert(t *model.Task) error {
	mt := lt.status.ap.Get().(*model.MongoTask)
	t.ConvertMongoTask(lt.status.c.cp, mt)
	lt.data = append(lt.data, mongo.NewInsertOneModel().SetDocument(mt))
	return nil
}

func (lt *lowerTransactionReal) InsertMany(ts []*model.Task) error {
	for _, t := range ts {
		mt := lt.status.ap.Get().(*model.MongoTask)
		t.ConvertMongoTask(lt.status.c.cp, mt)
		lt.data = append(lt.data, mongo.NewInsertOneModel().SetDocument(mt))
	}
	return nil
}

func (lt *lowerTransactionReal) Remove(uid string) error {
	lt.data = append(lt.data, mongo.NewDeleteOneModel().SetFilter(bson.M{"uid": uid}))
	return nil
}

func (lt *lowerTransactionReal) RemoveMany(uids []string) error {
	lt.data = append(lt.data, mongo.NewDeleteManyModel().SetFilter(bson.M{"uid": bson.M{"$in": uids}}))
	return nil
}

func (lt *lowerTransactionReal) Status(uid string, tt pb.TaskType) error {
	lt.data = append(
		lt.data,
		mongo.NewUpdateOneModel().
			SetFilter(bson.M{"uid": uid}).
			SetUpdate(bson.M{"$set": bson.M{"type": int32(tt)}}),
	)
	return nil
}

func (lt *lowerTransactionReal) NextTime(uid string, nt *time.Time) error {
	lt.data = append(
		lt.data,
		mongo.NewUpdateOneModel().
			SetFilter(bson.M{"uid": uid}).
			SetUpdate(bson.M{"$set": bson.M{"next_exec_time": nt.UnixNano()}}),
	)
	return nil
}

func (lt *lowerTransactionReal) IncrRetryTimes(uid string, num int) error {
	lt.data = append(
		lt.data,
		mongo.NewUpdateOneModel().
			SetFilter(bson.M{"uid": uid}).
			SetUpdate(bson.M{"$inc": bson.M{"retry_times": num}}),
	)
	return nil
}

func (lt *lowerTransactionReal) IncrSendTimes(uid string, num int) error {
	lt.data = append(
		lt.data,
		mongo.NewUpdateOneModel().
			SetFilter(bson.M{"uid": uid}).
			SetUpdate(bson.M{"$inc": bson.M{"times": num}}),
	)
	return nil
}
