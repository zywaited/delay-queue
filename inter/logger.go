package inter

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	pkgerr "github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	configAfters = append(configAfters, ConfigDataWithLogger)
}

type Format struct {
}

func (lf *Format) Format(entry *logrus.Entry) ([]byte, error) {
	out := &bytes.Buffer{}
	if entry.Buffer != nil {
		out = entry.Buffer
	}
	out.WriteString(entry.Time.Format("2006-01-02 15:04:05.000"))
	out.WriteString("\t")
	out.WriteString("[")
	out.WriteString(strings.ToUpper(entry.Level.String()))
	out.WriteString("]")
	out.WriteString("\t")
	out.WriteString(entry.Message)
	if len(entry.Data) > 0 {
		out.WriteString("\tDATA:{")
		i := 0
		for key, value := range entry.Data {
			out.WriteString(fmt.Sprintf("%s:%#v", key, value))
			i++
			if i < len(entry.Data) {
				out.WriteString(", ")
			}
		}
		out.WriteString("}")
	}

	out.WriteByte('\n')
	return out.Bytes(), nil
}

func ConfigDataWithLogger(cd *ConfigData) error {
	logConfig := cd.C.Log
	if logConfig == nil {
		return errors.New("logger's config is error")
	}
	path := strings.TrimSpace(logConfig.Dir)
	if path == "" {
		return errors.New("logger's dir is error")
	}
	path = strings.TrimRight(path, string(filepath.Separator))
	category := strings.TrimSpace(logConfig.Category)
	if category != "" {
		path = fmt.Sprintf("%s/%s", path, category)
	}

	// 初始化引擎
	eg := logrus.New()
	eg.SetFormatter(&Format{})
	eg.SetLevel(logrus.InfoLevel)
	eg.SetOutput(os.Stdout)

	// 创建目录
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return pkgerr.WithMessage(err, "logger create dir failed")
		}
	}

	// 打开文件
	fp := fmt.Sprintf("%s/%s", path, "%Y-%m-%d.log")
	rl, err := rotatelogs.New(fp)
	if nil != err {
		return fmt.Errorf("set logger rotate failed: %+v\n", err)
	}

	eg.SetOutput(rl)
	cd.CB.Logger = eg
	return nil
}
