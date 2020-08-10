package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

const defaultSeparationCharacter = "."

var handler = newHandler()

type Handler struct {
	// 后置处理器
	after []func(interface{})
	// 这里不加锁，先注册后使用
	coders map[string]Coder
}

func newHandler() *Handler {
	return &Handler{
		after:  make([]func(interface{}), 0),
		coders: make(map[string]Coder),
	}
}

func GetConfigHandler() *Handler {
	return handler
}

func (h *Handler) setCoder(coder Coder) {
	h.coders[coder.Name()] = coder
}

func (h *Handler) getCoder(name string) Coder {
	return h.coders[name]
}

func (h *Handler) validConfig(config interface{}) error {
	// 这里统一验证是否是有效指针（防止部分插件没做）
	rv := reflect.ValueOf(config)
	if rv.Kind() != reflect.Ptr {
		return fmt.Errorf("config non-pointer: %s", reflect.TypeOf(config))
	}
	if rv.IsNil() {
		return fmt.Errorf("config is nil: %s", reflect.TypeOf(config))
	}
	return nil
}

func (h *Handler) AddAfter(f func(interface{})) {
	h.after = append(h.after, f)
}

func (h *Handler) ClearAfter() {
	h.after = h.after[:0]
}

func (h *Handler) ParseFileWithName(name, file string, config interface{}) error {
	if err := h.validConfig(config); err != nil {
		return err
	}
	coder := h.getCoder(name)
	if coder == nil {
		return fmt.Errorf("not found %s'coder", name)
	}
	return errors.WithMessagef(coder.DecodeFile(file, config), "parse config failed: %s", coder.Name())
}

func (h *Handler) ParseWithName(name, data string, config interface{}) error {
	if err := h.validConfig(config); err != nil {
		return err
	}
	coder := h.getCoder(name)
	if coder == nil {
		return fmt.Errorf("not found %s'coder", name)
	}
	return errors.WithMessagef(coder.Decode(data, config), "parse config failed: %s", coder.Name())
}

func (h *Handler) Init(file string, config interface{}) error {
	index := strings.LastIndex(file, defaultSeparationCharacter)
	if index == -1 {
		return fmt.Errorf("config file has error suffix: %s", file)
	}
	err := h.ParseFileWithName(file[index+1:], file, config)
	if err != nil {
		return err
	}
	h.RunAfter(config)
	return nil
}

func (h *Handler) RunAfter(config interface{}) {
	for _, f := range h.after {
		f(config)
	}
}
