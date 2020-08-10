package transport

import "github.com/zywaited/delay-queue/protocol/pb"

type TransporterType string
type TransporterM map[TransporterType]Transporter

// 只需要发送即可
type Transporter interface {
	Valid(*pb.CallbackInfo) error
	Send(*pb.CallbackInfo, *pb.CallBackReq) error
}
