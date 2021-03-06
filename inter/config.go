package inter

import (
	"github.com/gomodule/redigo/redis"
	"github.com/zywaited/delay-queue/parser/system"
)

type Config struct {
	BaseLevel   string `toml:"base_level"`
	ConfigScale int64  `toml:"config_scale" valid:"required"` // 原始刻度
	Role        uint
	DataSource  *DataSourceConfig `toml:"data_source"`
	Timer       *TimerConfig
	Worker      *WorkerConfig
	Log         *LogConfig
	Redis       *RedisConnectConfig
	Services    *ServiceConfig
	GenerateId  *GenerateIdConfig `toml:"generate_id"`
}

type GenerateIdConfig struct {
	Type    string
	Timeout int64
	Group   *GroupConfig
}

type LogConfig struct {
	Dir      string // 日志路径
	Level    string
	StdPrint bool   `toml:"std_print"` // 是否打印到控制台
	Category string // 分类
}

type DataSourceConfig struct {
	Dst   string `valid:"required"` // data source type
	Redis *RedisStoreConfig
}

type RedisStoreConfig struct {
	Prefix string
	Name   string `valid:"required"`
}

type TimerConfig struct {
	St               string             // scanner type
	ConfigScaleLevel int64              `toml:"config_scale_level" valid:"required"` // 级别
	TimingWheel      *TimingWheelConfig `toml:"timing_wheel"`
	MaxCheckTime     int                `toml:"max_check_time"`
	Timeout          int64
	CheckMulti       int `toml:"check_multi"`
}

type GroupConfig struct {
	Id    string `valid:"required"`
	Group string `valid:"required"`
	Num   int    `valid:"required"`
}

type WorkerConfig struct {
	RetryTimes    int  `toml:"retry_times"`
	MultiNum      uint `toml:"multi_num" valid:"required"`
	Timeout       int64
	RepeatedTimes int64 `toml:"repeated_times"`
}

type TimingWheelConfig struct {
	MaxLevel          int   `toml:"max_level" valid:"required"` // 最大层级
	SlotNum           int   `toml:"slot_num" valid:"required"`
	ReloadGoNum       int   `toml:"reload_go_num" valid:"required"`
	ReloadConfigScale int64 `toml:"reload_config_scale" valid:"required"`
	ReloadPerNum      int   `toml:"reload_per_num" valid:"required"`
}

type RedisConnectConfig struct {
	Addr           string // 地址
	Auth           string // 密码
	Db             int    // 数据库
	Idle           int    // 最大连接数
	Active         int    // 一次性活跃
	Wait           bool   // 是否等待空闲连接
	ConnectTimeout int64  `toml:"connect_timeout"` // 连接超时时间， 毫秒
}

type ServiceConfig struct {
	Types []string           // GRPC\HTTP
	Wait  bool               // 是否等待
	HTTP  *HttpServiceConfig `toml:"http"`
	GRPC  *GRPCServiceConfig `toml:"grpc"`
}

type HttpServiceConfig struct {
	Addr string
}

type GRPCServiceConfig struct {
	Addr string
}

type ConfigBoot struct {
	Logger system.Logger
	Redis  *redis.Pool
}
