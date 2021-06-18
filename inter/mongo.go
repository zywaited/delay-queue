package inter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	pkgerr "github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func init() {
	configAfters = append(configAfters, ConfigDataWithMongo)
}

func ConfigDataWithMongo(cd *ConfigData) error {
	mongoConfig := cd.C.Mongo
	if mongoConfig == nil {
		return nil
	}
	if mongoConfig.DbName == "" {
		return errors.New("mongo db or collection name is empty")
	}
	ctx := context.Background()
	opts := options.Client().ApplyURI(mongoConfig.Uri)
	if mongoConfig.MaxPoolSize > 0 {
		opts.SetMaxPoolSize(mongoConfig.MaxPoolSize)
	}
	// 设置超时时间
	if mongoConfig.ConnectTimeout > 0 {
		opts.SetConnectTimeout(time.Duration(mongoConfig.ConnectTimeout) * time.Second)
		ctx, _ = context.WithTimeout(ctx, time.Duration(mongoConfig.ConnectTimeout)*time.Second)
	}
	if mongoConfig.MaxConnIdleTime > 0 {
		opts.SetMaxConnIdleTime(time.Duration(mongoConfig.MaxConnIdleTime) * time.Second)
	}
	// 读写分离
	opts.SetReadPreference(readpref.SecondaryPreferred())
	client, err := mongo.NewClient(opts)
	if err != nil {
		return pkgerr.WithMessage(err, "mongo init failed")
	}
	err = client.Connect(ctx)
	if err != nil {
		return pkgerr.WithMessage(err, "mongo connect failed")
	}
	cd.CB.Mongo.Client = client
	cd.CB.Mongo.Version, err = fetchMongoVersion(client.Database(mongoConfig.DbName))
	if err != nil {
		// ignore
		fmt.Println(pkgerr.WithMessage(err, "mongo fetch version failed"))
	}
	return nil
}

func fetchMongoVersion(db *mongo.Database) (int, error) {
	status, err := db.RunCommand(context.Background(), bson.D{{"serverStatus", 1}}).DecodeBytes()
	if err != nil {
		return 0, pkgerr.WithMessage(err, "mongo fetch serverStatus failed")
	}
	version, err := status.LookupErr("version")
	if err != nil {
		return 0, pkgerr.WithMessage(err, "mongo fetch serverStatus version failed")
	}
	versions := strings.SplitN(version.StringValue(), ".", 2)
	if len(versions) == 0 {
		return 0, errors.New("mongo parse version failed")
	}
	v, err := strconv.Atoi(versions[0])
	if err != nil {
		return 0, errors.New("mongo parse version string failed")
	}
	return v, nil
}
