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

	args := server.DequeueArgs{
		ConsumerId: 1,
	}
	var reply server.Message

	err = client.Call("Queue.Dequeue", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}

	fmt.Printf("[Consumer]: Recieved message: %v\n", reply)
}
