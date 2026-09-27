// Command backpressure demonstrates the specific way an unbounded
// per-connection queue costs memory when a client stalls, and how a
// bounded queue with an overflow policy avoids it.
//
// Go's http.ResponseWriter blocks on Write/Flush when the client (or a
// proxy) stops draining its socket — which is actually the *safe*
// default, compared to runtime environments that buffer writes silently.
// But that means a naive SSE server cannot call Write directly from
// whatever goroutine is publishing events (order handlers, a pub/sub
// callback, a fan-out loop): one slow subscriber would stall every
// publish. The fix nearly everyone reaches for is to decouple the
// publisher from the writer with a per-connection queue — and if that
// queue has no cap, a client that stops reading (an asleep phone, a
// backgrounded tab) turns it into unbounded memory growth: exactly
// sse-kit's Node problem, reproduced in Go's version of the same
// architecture. ssegap's Session bounds that queue by construction; this
// program builds the naive (unbounded) version alongside it to measure
// the difference under identical load.
//
// Run it:
//
//	go run ./examples/backpressure
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"time"

	ssegap "github.com/agentscope-ai-java/ssegap"
)

const (
	events     = 20000
	eventBytes = 2 * 1024 // ~2 KB per event, matching sse-kit's Node demo
)

func payload() []byte {
	b := make([]byte, eventBytes)
	for i := range b {
		b[i] = 'x'
	}
	return b
}

// naiveQueue is the unbounded version of ssegap's outbox: the pattern a
// hand-rolled SSE server reaches for to keep a slow subscriber from
// blocking whoever is publishing events, minus the one line that bounds
// it. Structurally identical to ssegap's internal outbox, deliberately,
// so the only difference measured below is the presence of a cap.
type naiveQueue struct {
	mu     sync.Mutex
	queue  [][]byte
	signal chan struct{}
}

func newNaiveQueue() *naiveQueue {
	return &naiveQueue{signal: make(chan struct{}, 1)}
}

func (q *naiveQueue) push(frame []byte) {
	q.mu.Lock()
	q.queue = append(q.queue, frame) // <-- no capacity check
	q.mu.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

func (q *naiveQueue) drain() [][]byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	f := q.queue
	q.queue = nil
	return f
}

func serveNaive(w http.ResponseWriter, r *http.Request, q *naiveQueue) {
	flusher := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-q.signal:
			for _, f := range q.drain() {
				if _, err := w.Write(f); err != nil {
					return
				}
				flusher.Flush() // blocks here once the stalled client's kernel buffer fills
			}
		}
	}
}

// stalledClient connects to url and never reads the response body,
// simulating a client that stopped draining its socket.
func stalledClient(url string) (stop func()) {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	return func() { resp.Body.Close() }
}

func heapKB() int64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc) / 1024
}

func runNaive(data []byte) (beforeKB, afterKB int64) {
	q := newNaiveQueue()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveNaive(w, r, q)
	}))
	defer srv.Close()

	stop := stalledClient(srv.URL)
	defer stop()
	time.Sleep(20 * time.Millisecond) // let the handler goroutine start and flush headers

	beforeKB = heapKB()
	for i := 0; i < events; i++ {
		q.push(append([]byte(nil), data...)) // copy, like Session.Send does for its frame
	}
	// Give the writer goroutine a moment to drain whatever the kernel
	// buffer had room for, then measure: the rest sits in q.queue,
	// unbounded, waiting for a client that is never coming back.
	time.Sleep(200 * time.Millisecond)
	afterKB = heapKB()
	return beforeKB, afterKB
}

func runBounded(data []byte) (beforeKB, afterKB int64) {
	connected := make(chan *ssegap.Session, 1)
	h := ssegap.NewHandler(ssegap.Options{
		Heartbeat:      -1,
		QueueCapacity:  256,
		OverflowPolicy: ssegap.DropOldest,
		OnConnect:      func(s *ssegap.Session) { connected <- s },
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	stop := stalledClient(srv.URL)
	defer stop()
	s := <-connected

	beforeKB = heapKB()
	for i := 0; i < events; i++ {
		s.Send(ssegap.Message{Data: string(data)})
	}
	time.Sleep(200 * time.Millisecond)
	afterKB = heapKB()
	return beforeKB, afterKB
}

func main() {
	fmt.Printf("pushing %d events (~%d bytes each) to a client that never reads\n\n", events, eventBytes)

	data := payload()
	nb, na := runNaive(data)
	bb, ba := runBounded(data)

	fmt.Printf("naive (unbounded queue)   heap grew %+d KB\n", na-nb)
	fmt.Printf("bounded (ssegap, cap 256) heap grew %+d KB\n", ba-bb)
}
