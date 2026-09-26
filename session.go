package ssegap

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Session represents one open SSE connection. A Handler creates one per
// request and hands it to Options.OnConnect (and, for a detected gap,
// Options.OnGap); everything an application does with a live connection —
// pushing events, reacting to disconnect — goes through it.
type Session struct {
	req *http.Request
	w   http.ResponseWriter
	f   http.Flusher

	log EventLog

	outbox *outbox

	closeOnce sync.Once
	stopped   chan struct{} // closed by Close or a Disconnect-policy overflow

	writerDone chan struct{} // closed once the writer goroutine has returned
}

func newSession(r *http.Request, w http.ResponseWriter, f http.Flusher, log EventLog, queueCapacity int, policy OverflowPolicy) *Session {
	return &Session{
		req:        r,
		w:          w,
		f:          f,
		log:        log,
		outbox:     newOutbox(queueCapacity, policy),
		stopped:    make(chan struct{}),
		writerDone: make(chan struct{}),
	}
}

// Send writes an event to the client. If the Handler was configured with
// an EventLog, Send persists msg first (which assigns msg.ID when it was
// left empty) so the id actually on the wire is always one the log can
// later answer for in Since; otherwise msg is framed exactly as given.
//
// Send never blocks on the client: the frame is handed to a bounded,
// per-connection queue and a dedicated writer goroutine does the
// (potentially slow) write. Once the connection has ended, Send is a
// no-op.
func (s *Session) Send(msg Message) {
	if s.log != nil {
		msg = s.log.Append(msg)
	}
	s.enqueue(appendMessage(nil, msg))
}

// Comment sends a raw ": text" comment line. EventSource ignores comments
// (they never reach application code, no "message" event fires) but they
// still count as traffic on the wire, which is what makes them useful as
// heartbeats against idle-connection timeouts in proxies and load
// balancers.
func (s *Session) Comment(text string) {
	s.enqueue(appendComment(nil, text))
}

func (s *Session) enqueue(frame []byte) {
	select {
	case <-s.stopped:
		return
	default:
	}
	if s.outbox.push(frame) {
		s.forceClose()
	}
}

// Close ends the connection from the server side. It is safe to call more
// than once and from any goroutine; only the first call has an effect.
func (s *Session) Close() {
	s.forceClose()
}

func (s *Session) forceClose() {
	s.closeOnce.Do(func() {
		close(s.stopped)
	})
}

// Done returns a channel that is closed when the connection ends, whether
// because the client disconnected, the request's context was canceled, or
// the server called Close. Application code that registered anything on
// this session's behalf (a pub/sub subscription, a timer) should select on
// Done to clean it up — see the package README for the full pattern.
func (s *Session) Done() <-chan struct{} {
	return s.stopped
}

// Request returns the *http.Request this session was created from, e.g.
// to read query parameters or headers.
func (s *Session) Request() *http.Request {
	return s.req
}

// run drives the connection: it starts the single writer goroutine, waits
// for the connection to end (client gone, context canceled, or Close
// called), and does not return until that writer goroutine has fully
// exited — so that when run returns, this connection has left behind
// exactly zero goroutines.
func (s *Session) run(ctx context.Context, heartbeat time.Duration) {
	go s.writeLoop(heartbeat)

	select {
	case <-ctx.Done():
	case <-s.stopped:
	}
	s.forceClose() // no-op if already closed; covers the ctx.Done() path
	s.outbox.shutdown()
	<-s.writerDone
}

// writeLoop is the sole goroutine that ever writes to s.w. Centralizing
// writes here means a stalled client — one where http.Flusher.Flush
// blocks because the kernel's send buffer is full — only ever blocks this
// one goroutine, never the caller of Send.
func (s *Session) writeLoop(heartbeat time.Duration) {
	defer close(s.writerDone)

	var tick <-chan time.Time
	if heartbeat > 0 {
		t := time.NewTicker(heartbeat)
		defer t.Stop()
		tick = t.C
	}

	for {
		select {
		case <-s.stopped:
			// Drain whatever is left once, best-effort, then exit.
			// A client that's still reading gets a clean final
			// batch; a stalled one gets nothing further, which is
			// the point.
			for _, frame := range s.outbox.drain() {
				if !s.write(frame) {
					return
				}
			}
			return
		case <-s.outbox.signal:
			for _, frame := range s.outbox.drain() {
				if !s.write(frame) {
					return
				}
			}
		case <-tick:
			s.enqueue(appendComment(nil, "heartbeat"))
		}
	}
}

// write writes one frame and flushes it, reporting whether the write
// succeeded. Flush blocks until the client (or an intermediary) has
// accepted the bytes, which is exactly why it only ever happens on this
// goroutine.
func (s *Session) write(frame []byte) bool {
	if _, err := s.w.Write(frame); err != nil {
		return false
	}
	s.f.Flush()
	return true
}
