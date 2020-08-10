package inter

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	pkgerror "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/parser/config"
)

var configAfters = []func(*ConfigData) error{func(cd *ConfigData) error {
	bs, _ := json.Marshal(cd.C)
	fmt.Printf("init config: %s \n", string(bs))
	return nil
}}

type ConfigData struct {
	C  *Config
	CB *ConfigBoot
}

// 1: 全局变量只是用来初始化
// 2: 全局变量只是为了保存下数据，方便回溯
// 3: 程序逻辑上不能依赖全局变量
// 所以这里采用每次NEW进行生成

type GenerateConfigOption func(*GenerateConfig)

func GenerateConfigWithConfigName(configName string) GenerateConfigOption {
	return func(gc *GenerateConfig) {
		gc.configName = strings.TrimSpace(configName)
	}
}

func GenerateConfigWithConfigType(configType string) GenerateConfigOption {
	return func(gc *GenerateConfig) {
		gc.configType = strings.TrimSpace(configType)
	}
}

func GenerateConfigWithAfter(after ...func(*ConfigData) error) GenerateConfigOption {
	return func(gc *GenerateConfig) {
		gc.after = after
	}
}

type GenerateConfig struct {
	configName string
	configType string

	cd *ConfigData

	after  []func(*ConfigData) error
	parsed bool
}

func NewGenerateConfigWithDefaultAfters(opts ...GenerateConfigOption) *GenerateConfig {
	opts = append(
		append([]GenerateConfigOption{}, GenerateConfigWithAfter(configAfters...)),
		opts...,
	)
	return NewGenerateConfig(opts...)
}

func NewGenerateConfig(opts ...GenerateConfigOption) *GenerateConfig {
	gc := &GenerateConfig{cd: &ConfigData{
		C:  &Config{},
		CB: &ConfigBoot{},
	}}
	for _, opt := range opts {
		opt(gc)
	}
	return gc
}

// 这里需要先执行Run
func (gc *GenerateConfig) ConfigData() *ConfigData {
	return gc.cd
}

// 这里不能出现并发(ch是公用的)
func (gc *GenerateConfig) Run() (err error) {
	if gc.parsed {
		return
	}
	if gc.configName == "" {
		return errors.New("配置文件参数为空")
	}
	ch := config.GetConfigHandler()
	if gc.configType != "" {
		err = ch.ParseFileWithName(gc.configType, gc.configName, gc.cd.C)
	} else {
		err = ch.Init(gc.configName, gc.cd.C)
	}
	if err != nil {
		return pkgerror.WithMessagef(err, "处理配置文件出错")
	}
	for _, after := range gc.after {
		if err = after(gc.cd); err != nil {
			err = pkgerror.WithMessagef(err, "处理配置出错")
			return
		}
	}
	gc.parsed = true
	return
}
