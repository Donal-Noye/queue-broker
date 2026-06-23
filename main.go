package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type waiter struct {
	ch chan string
}

type queue struct {
	messages []string
	waiters  []waiter
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{
		queues: make(map[string]*queue),
	}
}

func (b *broker) getOrCreate(name string) *queue {
	q, ok := b.queues[name]
	if !ok {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

func (b *broker) put(name string, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.getOrCreate(name)
	if len(q.waiters) > 0 {
		w := q.waiters[0]
		q.waiters = q.waiters[1:]
		ch := w.ch
		ch <- msg

		return
	}

	q.messages = append(q.messages, msg)
}

func (b *broker) get(ctx context.Context, name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()

	q := b.getOrCreate(name)
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		b.mu.Unlock()
		return msg, true
	}

	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}

	w := waiter{ch: make(chan string, 1)}
	q.waiters = append(q.waiters, w)

	b.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-w.ch:
		return msg, true
	case <-ctx.Done():
		b.mu.Lock()
		select {
		case msg := <-w.ch:
			q.messages = append([]string{msg}, q.messages...)
			b.mu.Unlock()
			return "", false
		default:
		}

		for i, qw := range q.waiters {
			if qw.ch == w.ch {
				q.waiters = append(
					q.waiters[:i],
					q.waiters[i+1:]...,
				)
				break
			}
		}

		b.mu.Unlock()
		return "", false
	case <-timer.C:
		b.mu.Lock()
		select {
		case msg := <-w.ch:
			b.mu.Unlock()
			return msg, true
		default:
		}

		for i, qw := range q.waiters {
			if qw.ch == w.ch {
				q.waiters = append(
					q.waiters[:i],
					q.waiters[i+1:]...,
				)
				break
			}
		}

		b.mu.Unlock()
		return "", false
	}
}

func handler(b *broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path[1:]
		if name == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodPut:
			msg := r.URL.Query().Get("v")
			if msg == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			b.put(name, msg)
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			var timeout time.Duration
			if s := r.URL.Query().Get("timeout"); s != "" {
				n, err := strconv.Atoi(s)
				if err != nil || n <= 0 {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				timeout = time.Duration(n) * time.Second
			}
			msg, ok := b.get(r.Context(), name, timeout)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			fmt.Fprintln(w, msg)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: broker <port>")
		os.Exit(1)
	}

	b := newBroker()
	mux := http.NewServeMux()
	mux.HandleFunc("/", handler(b))

	if err := http.ListenAndServe(":"+os.Args[1], mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
