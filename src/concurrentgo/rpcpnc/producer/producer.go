package main

import (
	"fmt"
	"log"
	"net/rpc"

	"6.5840/concurrentgo/rpcpnc/server"
)

func main() {
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	args := server.EnqueuArgs{
		ProdcuerId: 1,
		Msg: server.Message{
			Payload: []byte("hello world"),
			Subject: "subject-1",
		},
	}

	var reply bool

	err = client.Call("Queue.Enqueue", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}

	if reply {
		fmt.Print("[Producer]: Enqueued message successfully!")
	}
}
