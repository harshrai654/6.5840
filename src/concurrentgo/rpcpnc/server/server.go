package server

import (
	"fmt"
	"log"
	"sync"
)

type Message struct {
	Payload []byte
	Subject string
}

type EnqueuArgs struct {
	Msg        Message
	ProdcuerId int
}

type DequeueArgs struct {
	ConsumerId int
}

type Queue struct {
	Messages          []Message
	ProductionPointer int
	ConsumetPointer   int
	Count             int
	Pcv               sync.Cond
	Ccv               sync.Cond
}

func (q *Queue) Enqueue(args *EnqueuArgs, reply *bool) error {
	q.Pcv.L.Lock()
	for q.Count == len(q.Messages) {
		log.Printf("[producer: %d]: Queue is full, waiting for cosumption...\n", args.ProdcuerId)
		q.Pcv.Wait()
	}

	q.Messages[q.ProductionPointer] = args.Msg
	log.Printf("[producer: %d]: Enqueued message at %d pointer\n", args.ProdcuerId, q.ProductionPointer)
	q.ProductionPointer = (q.ProductionPointer + 1) % len(q.Messages)
	q.Count++

	q.Ccv.Signal()
	q.Pcv.L.Unlock()

	*reply = true

	return nil
}

func (q *Queue) Dequeue(args *DequeueArgs, msg *Message) error {
	q.Ccv.L.Lock()
	for q.Count == 0 {
		log.Printf("[consumer:%d]: Queue is empty, waiting for production\n", args.ConsumerId)
		q.Ccv.Wait()
	}

	*msg = q.Messages[q.ConsumetPointer]
	fmt.Printf("[consumer: %d]: Dequeued message at %d pointer\n", args.ConsumerId, q.ConsumetPointer)
	q.ConsumetPointer = (q.ConsumetPointer + 1) % len(q.Messages)
	q.Count--

	log.Printf("[consumer:%d]: Consumed Element with payload: `%s` | subject: %s\n", args.ConsumerId, string(msg.Payload), msg.Subject)

	q.Pcv.Signal()
	q.Ccv.L.Unlock()

	return nil
}
