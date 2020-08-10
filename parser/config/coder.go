package config

type Coder interface {
	Name() string
	DecodeFile(string, interface{}) error
	Decode(string, interface{}) error
}
