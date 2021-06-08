package main

import "github.com/zywaited/delay-queue/server"

func main() {
	srv := server.NewDelayQueue(server.AcquireServerInits()...)
	panic(srv.Run())
}
