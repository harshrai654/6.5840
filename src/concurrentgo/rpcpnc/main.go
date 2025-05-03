package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	"6.5840/concurrentgo/rpcpnc/server"
)

func main() {
	bufferSize := flag.Int("buffer_size", 100, "Queue size")
	flag.Parse()

	var mu sync.Mutex
	// Initiliase queue
	q := server.Queue{
		Messages:          make([]server.Message, *bufferSize),
		ProductionPointer: 0,
		ConsumetPointer:   0,
		Pcv:               *sync.NewCond(&mu),
		Ccv:               *sync.NewCond(&mu),
	}

	rpc.Register(&q)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)

	select {}
}
