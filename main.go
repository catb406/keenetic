package main

import (
	"container/list"
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	safeQueue struct {
		queues map[string]*list.List // key: queue name - value: queue values
		mu     *sync.Mutex
	}

	queue interface {
		push(k string, v interface{})
		pop(k string) (interface{}, bool)
	}

	waitChan struct {
		// data channel is used to transfer values directly to receiver if there are any waiting for response
		data chan string
		// stop channel is used for notifying sender to not send value to data channel
		// if receiver is not waiting for response anymore
		stop chan struct{}
	}
)

func (q *safeQueue) push(k string, v interface{}) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if qu, ok := q.queues[k]; ok {
		qu.PushBack(v)
		return
	}
	l := list.New()
	l.PushBack(v)
	q.queues[k] = l
}

func (q *safeQueue) pop(k string) (interface{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if qu, ok := q.queues[k]; ok {
		val := qu.Front()
		if val == nil {
			return nil, false
		}
		qu.Remove(val)
		return val.Value, true
	}
	return nil, false
}

func newQueue() queue {
	return &safeQueue{
		queues: make(map[string]*list.List),
		mu:     &sync.Mutex{},
	}
}

func newWaitChan() waitChan {
	return waitChan{
		data: make(chan string),
		stop: make(chan struct{}),
	}
}

func main() {
	port := os.Args[1]

	values := newQueue()
	waiters := newQueue()

	http.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		queueName, ok := strings.CutPrefix(request.URL.Path, "/")
		if !ok || queueName == "" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		ctx := request.Context()
		switch request.Method {
		case http.MethodGet:
			if val, ok := values.pop(queueName); ok {
				// there is value in queue, return it
				writer.Write([]byte(val.(string)))
				return
			}

			timeout, ok := request.URL.Query()["timeout"]
			if !ok {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			if len(timeout) != 1 {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			timeoutSecs, err := strconv.Atoi(timeout[0])
			if err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Second*time.Duration(timeoutSecs))
			defer cancel()

			c := newWaitChan()
			// add new receiver to the end of the queue
			waiters.push(queueName, c)
			select {
			// user timeout exceeded or request was cancelled, no need to wait result for this receiver
			case <-ctx.Done():
				close(c.stop)
				writer.WriteHeader(http.StatusNotFound)
			case val := <-c.data:
				writer.Write([]byte(val))
			}
			return
		case http.MethodPut:
			v, ok := request.URL.Query()["v"]
			if !ok || len(v) != 1 {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			value := v[0]
			if value == "" {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
		senderLoop:
			for {
				// get first receiver from queue if there are any
				if w, ok := waiters.pop(queueName); ok {
					c := w.(waitChan)
					select {
					// channel is closed, skip this receiver
					case <-c.stop:
						continue
					case c.data <- value:
						break senderLoop
						// request cancelled
					case <-ctx.Done():
						break senderLoop
					}
				} else {
					// no active receivers, add to queue
					values.push(queueName, value)
					break
				}
			}
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
