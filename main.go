package main

import "github.com/zywaited/delay-queue/server"

func main() {
	srv := server.NewDelayQueue()
	panic(srv.Run())
}
