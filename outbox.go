package ssegap

import "sync"

// OverflowPolicy controls what happens when a connection's outbound queue
// is full because the client isn't draining it fast enough.
type OverflowPolicy int

const (
	// DropOldest discards the oldest queued (not-yet-written) frame to
	// make room for the new one. Good for feeds where the latest state
	// matters more than a complete history — a live dashboard, a
	// ticker.
	DropOldest OverflowPolicy = iota

	// DropNewest discards the incoming frame and keeps what is already
	// queued. Good when the order and completeness of what's already
	// in flight matters more than freshness.
	DropNewest

	// Disconnect closes the connection instead of dropping anything.
	// Good when a full queue means the client is so far behind that
	// partial data would be misleading, and reconnecting (to get a
	// clean replay or an honest gap signal) is better than continuing
	// to fall further behind silently.
	Disconnect
)

// outbox is the bounded, per-connection queue of already-framed bytes
// waiting to be written. It exists so a stalled client (one whose
// http.Flusher.Flush would block, because the kernel's send buffer is
// full) cannot make the producer side of a connection — Session.Send,
// called from arbitrary application goroutines — block or allocate
// without limit. Everything ever pushed through push has already been
// durably recorded by the EventLog (if one is configured) before it
// reaches here, so DropOldest/DropNewest only ever discard bytes that
// were never handed to the socket, never events a client could not later
// recover through replay or a gap signal.
type outbox struct {
	mu       sync.Mutex
	queue    [][]byte
	capacity int
	policy   OverflowPolicy
	closed   bool
	forced   bool // true when Disconnect policy tripped

	// signal is buffered size 1 and used purely as a doorbell: a send
	// on it (non-blocking, best-effort) wakes the writer goroutine if
	// it's waiting; a full channel means it's already been rung and
	// hasn't been answered yet, which is fine since the writer drains
	// the whole queue each time it wakes.
	signal chan struct{}
}

func newOutbox(capacity int, policy OverflowPolicy) *outbox {
	if capacity <= 0 {
		capacity = 256
	}
	return &outbox{
		capacity: capacity,
		policy:   policy,
		signal:   make(chan struct{}, 1),
	}
}

// push enqueues frame, applying the overflow policy if the queue is
// already at capacity. It reports whether the connection should be
// force-closed as a result (only possible under the Disconnect policy,
// and only once).
func (o *outbox) push(frame []byte) (forceClose bool) {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return false
	}

	if len(o.queue) >= o.capacity {
		switch o.policy {
		case DropOldest:
			copy(o.queue, o.queue[1:])
			o.queue[len(o.queue)-1] = frame
			o.mu.Unlock()
			o.ring()
			return false
		case DropNewest:
			o.mu.Unlock()
			return false // incoming frame discarded, queue unchanged
		case Disconnect:
			already := o.forced
			o.forced = true
			o.mu.Unlock()
			return !already
		}
	}

	o.queue = append(o.queue, frame)
	o.mu.Unlock()
	o.ring()
	return false
}

// ring wakes the writer goroutine, if it is waiting for one.
func (o *outbox) ring() {
	select {
	case o.signal <- struct{}{}:
	default:
	}
}

// drain removes and returns every currently queued frame, oldest first.
func (o *outbox) drain() [][]byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.queue) == 0 {
		return nil
	}
	q := o.queue
	o.queue = nil
	return q
}

// shutdown marks the outbox closed; further pushes are silently dropped.
func (o *outbox) shutdown() {
	o.mu.Lock()
	o.closed = true
	o.mu.Unlock()
}
