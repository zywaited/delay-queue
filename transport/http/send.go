package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	pkgerr "github.com/pkg/errors"
	"github.com/zywaited/delay-queue/protocol/pb"
)

const (
	SendersType     = "HTTP"
	SenderHttpsType = "HTTPS"
)

type SenderOption func(*Sender)

func SenderOptionWithTimeout(timeout time.Duration) SenderOption {
	return func(s *Sender) {
		s.timeout = timeout
	}
}

type Sender struct {
	s *http.Client

	bp *sync.Pool

	timeout time.Duration
}

func NewSender(opts ...SenderOption) *Sender {
	s := &Sender{
		bp: &sync.Pool{New: func() interface{} {
			return bytes.NewBuffer(nil)
		}},
	}
	for _, opt := range opts {
		opt(s)
	}
	c := resty.New()
	if s.timeout > 0 {
		c.SetTimeout(s.timeout)
	}
	s.s = c.GetClient()
	return s
}

func (s *Sender) Send(addr *pb.CallbackInfo, req *pb.CallBackReq) error {
	req.Url = s.uri(addr)
	bs, err := json.Marshal(req)
	if err != nil {
		return errors.New("协议转换JSON失败")
	}
	bt := s.bp.Get().(*bytes.Buffer)
	defer func() {
		bt.Reset()
		s.bp.Put(bt)
	}()
	bt.Write(bs)
	resp, err := s.s.Post(req.Url, "application/json", bt)
	if err != nil {
		return pkgerr.WithMessagef(err, "HTTP SEND failed: %s: %v", bs, err)
	}
	// fix: 防止句柄过多
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("HTTP SEND failed: %d", resp.StatusCode)
	}
	return nil
}

func (s *Sender) Valid(addr *pb.CallbackInfo) error {
	uri := s.uri(addr)
	_, err := url.Parse(uri)
	return pkgerr.WithMessage(err, "HTTP SENDER Parse URI failed")
}

func (s *Sender) uri(addr *pb.CallbackInfo) string {
	return strings.ToLower(strings.TrimSpace(addr.Schema)) +
		"://" + strings.TrimSpace(addr.Address) + "/" +
		strings.TrimLeft(strings.TrimSpace(addr.Path), "/")
}
