package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

type Message struct {
	payload []byte
	subject string
}

type Queue struct {
	messages          []Message
	productionPointer int
	consumetPointer   int
	count             int
	pcv               sync.Cond
	ccv               sync.Cond
}

func (q *Queue) Enqueue(msg *Message, producerId int) {
	q.messages[q.productionPointer] = *msg
	fmt.Printf("[producer: %d]: Enqueued message at %d pointer\n", producerId, q.productionPointer)
	q.productionPointer = (q.productionPointer + 1) % len(q.messages)
	q.count++
}

func (q *Queue) Dequeue(consumerId int) *Message {
	msg := q.messages[q.consumetPointer]
	fmt.Printf("[consumer: %d]: Dequeued message at %d pointer\n", consumerId, q.consumetPointer)
	q.consumetPointer = (q.consumetPointer + 1) % len(q.messages)
	q.count--

	return &msg
}

func main() {
	bufferSize := flag.Int("buffer_size", 100, "Queue size")
	producerCount := flag.Int("producer_count", 10, "Number of producers")
	consumerCount := flag.Int("consumer_count", 10, "Number of consumers")
	flag.Parse()

	var mu sync.Mutex
	// Initiliase queue
	q := Queue{
		messages:          make([]Message, *bufferSize),
		productionPointer: 0,
		consumetPointer:   0,
		pcv:               *sync.NewCond(&mu),
		ccv:               *sync.NewCond(&mu),
	}

	var wg sync.WaitGroup

	for i := range *producerCount {
		wg.Add(1)
		go producer(&q, i)
	}

	for i := range *consumerCount {
		wg.Add(1)
		go consumer(&q, i)
	}

	wg.Wait()
}

func consumer(q *Queue, consumerId int) {
	for {
		q.ccv.L.Lock()
		for q.count == 0 {
			fmt.Printf("[consumer:%d]: Queue is empty, waiting for production\n", consumerId)
			q.ccv.Wait()
		}

		msg := q.Dequeue(consumerId)

		fmt.Printf("[consumer:%d]: Consumed Element with payload: `%s` | subject: %s\n", consumerId, string(msg.payload), msg.subject)

		q.pcv.Signal()
		q.ccv.L.Unlock()

		time.Sleep(2 * time.Second)
	}
}

func producer(q *Queue, producerId int) {
	for {
		q.pcv.L.Lock()
		for q.count == len(q.messages) {
			fmt.Printf("[producer: %d]: Queue is full, waiting for cosumption...\n", producerId)
			q.pcv.Wait()
		}

		value := rand.Int()

		q.Enqueue(&Message{
			payload: fmt.Appendf(nil, "message: %d", value),
			subject: "Producer payload",
		}, producerId)

		q.ccv.Signal()
		q.pcv.L.Unlock()

		time.Sleep(2 * time.Second)
	}
}
