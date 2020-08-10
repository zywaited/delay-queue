package config

import (
	"github.com/BurntSushi/toml"
	"github.com/pkg/errors"
)

func init() {
	GetConfigHandler().setCoder(NewTomlCoder())
}

type tomlCoder struct {
}

func NewTomlCoder() *tomlCoder {
	return &tomlCoder{}
}

func (t *tomlCoder) Name() string {
	return "toml"
}

func (t *tomlCoder) DecodeFile(file string, config interface{}) error {
	_, err := toml.DecodeFile(file, config)
	return errors.WithMessage(err, "toml config parse file failed")
}

func (t *tomlCoder) Decode(data string, config interface{}) error {
	_, err := toml.Decode(data, config)
	return errors.WithMessage(err, "toml config parse data failed")
}
