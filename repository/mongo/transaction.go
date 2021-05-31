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
)

// version >= 4.0
type transaction struct {
	*status
}

func NewTransactionStore(client *mongo.Client, opts ...ConfigOption) *transaction {
	return &transaction{status: NewStatusStore(client, opts...)}
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
	}()
	return errors.WithMessage(fn(t), "mongo业务逻辑出错[Transaction]")
}

// version < 4.0 [单表不重试]
type lowerTransaction struct {
	*status

	// 用户中间数据存储
	ap   *sync.Pool
	data []mongo.WriteModel
}

func NewLowerTransactionStore(client *mongo.Client, opts ...ConfigOption) *lowerTransaction {
	return &lowerTransaction{
		status: NewStatusStore(client, opts...),
		ap: &sync.Pool{New: func() interface{} {
			return &lowerTransaction{}
		}},
	}
}

// 主要实现修改相关操作
func (lt *lowerTransaction) Insert(t *model.Task) error {
	lt.data = append(lt.data, mongo.NewInsertOneModel().SetDocument(t))
	return nil
}

func (lt *lowerTransaction) InsertMany(ts []*model.Task) error {
	for _, t := range ts {
		lt.data = append(lt.data, mongo.NewInsertOneModel().SetDocument(t))
	}
	return nil
}

func (lt *lowerTransaction) Remove(uid string) error {
	lt.data = append(lt.data, mongo.NewDeleteOneModel().SetFilter(bson.M{"uid": uid}))
	return nil
}

func (lt *lowerTransaction) RemoveMany(uids []string) error {
	lt.data = append(lt.data, mongo.NewDeleteManyModel().SetFilter(bson.M{"uid": bson.M{"$in": uids}}))
	return nil
}

func (lt *lowerTransaction) Status(uid string, tt pb.TaskType) error {
	lt.data = append(
		lt.data,
		mongo.NewUpdateOneModel().
			SetFilter(bson.M{"uid": uid}).
			SetUpdate(bson.M{"$set": bson.M{"type": int32(tt)}}),
	)
	return nil
}

func (lt *lowerTransaction) NextTime(uid string, nt *time.Time) error {
	lt.data = append(
		lt.data,
		mongo.NewUpdateOneModel().
			SetFilter(bson.M{"uid": uid}).
			SetUpdate(bson.M{"$set": bson.M{"next_exec_time": nt.UnixNano()}}),
	)
	return nil
}

func (lt *lowerTransaction) IncrRetryTimes(uid string, num int) error {
	lt.data = append(
		lt.data,
		mongo.NewUpdateOneModel().
			SetFilter(bson.M{"uid": uid}).
			SetUpdate(bson.M{"$inc": bson.M{"retry_times": num}}),
	)
	return nil
}

func (lt *lowerTransaction) IncrSendTimes(uid string, num int) error {
	lt.data = append(
		lt.data,
		mongo.NewUpdateOneModel().
			SetFilter(bson.M{"uid": uid}).
			SetUpdate(bson.M{"$inc": bson.M{"times": num}}),
	)
	return nil
}

func (lt *lowerTransaction) Transaction(fn func(role.DataStore) error) (err error) {
	nt := lt.ap.Get().(*lowerTransaction)
	nt.status = lt.status
	defer func() {
		if re := recover(); re != nil {
			err = fmt.Errorf("mongo lower transaction: %v, stack: %s", re, system.Stack())
		}
		nt.status = nil
		nt.data = nt.data[:0]
		lt.ap.Put(nt)
	}()
	err = errors.WithMessage(fn(nt), "mongo业务逻辑出错[LowerTransaction]")
	if err != nil || len(nt.data) == 0 {
		return
	}
	_, err = lt.status.collection.BulkWrite(context.Background(), nt.data)
	if err != nil {
		err = errors.WithMessage(fn(nt), "mongo bulk write 逻辑出错[LowerTransaction]")
	}
	return
}
