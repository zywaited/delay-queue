package inter

import (
	"errors"
	"strings"

	"github.com/asaskevich/govalidator"
	pkgerr "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/role"
	"github.com/zywaited/delay-queue/role/timer/sorted"
	tw "github.com/zywaited/delay-queue/role/timer/timing-wheel"
)

func init() {
	configAfters = append(configAfters, ConfigDataWithValidatorAddr)
	configAfters = append(configAfters, ConfigDataWithValidatorTimer)
	configAfters = append(configAfters, ConfigDataWithValidatorWorker)
	configAfters = append(configAfters, ConfigDataWithValidatorStore)
}

const (
	GRPCSchema = "GRPC"
	HTTPSchema = "HTTP"
)

var validSchema = map[string]bool{
	GRPCSchema: true,
	HTTPSchema: true,
}

func ConfigDataWithValidatorAddr(cd *ConfigData) error {
	if cd.C.Role&uint(role.Timer) == 0 {
		return nil
	}
	if cd.C.Services == nil || len(cd.C.Services.Types) == 0 {
		return errors.New("未指定服务类型")
	}
	for _, schema := range cd.C.Services.Types {
		schema = strings.ToUpper(strings.TrimSpace(schema))
		if !validSchema[schema] {
			return errors.New("服务类型有误")
		}
		// todo 后续验证IP或者URI地址
	}
	return nil
}

var validTimerName = map[string]bool{
	string(tw.ServerName):     true,
	string(sorted.ServerName): true,
}

func ConfigDataWithValidatorWorker(cd *ConfigData) error {
	if cd.C.Role&uint(role.Worker) == 0 {
		return nil
	}
	_, err := govalidator.ValidateStruct(cd.C.Worker)
	return pkgerr.WithMessage(err, "worker's config is error")
}

func ConfigDataWithValidatorTimer(cd *ConfigData) error {
	if cd.C.Role&uint(role.Timer) == 0 {
		return nil
	}
	_, err := govalidator.ValidateStruct(cd.C.Timer)
	if err != nil {
		return pkgerr.WithMessage(err, "timer's config is error")
	}
	if !validTimerName[cd.C.Timer.St] {
		return errors.New("timer's type is error")
	}
	if cd.C.Timer.St != string(tw.ServerName) {
		return nil
	}
	_, err = govalidator.ValidateStruct(cd.C.Timer.TimingWheel)
	return pkgerr.WithMessage(err, "timing-wheel's config is error")
}

const (
	RedisStore  = "redis"
	MongoStore  = "mongo"
	MemoryStore = "memory"
)

var validStore = map[string]bool{
	RedisStore: true,
	MongoStore: true,
}

func ConfigDataWithValidatorStore(cd *ConfigData) error {
	_, err := govalidator.ValidateStruct(cd.C.DataSource)
	if err != nil {
		return pkgerr.WithMessage(err, "store's config is error")
	}
	if !validStore[cd.C.DataSource.Dst] {
		return pkgerr.WithMessage(err, "store's type is error")
	}
	if cd.C.DataSource.Dst != RedisStore {
		return nil
	}
	_, err = govalidator.ValidateStruct(cd.C.DataSource.Redis)
	return pkgerr.WithMessage(err, "redis store's config is error")
}
