package ssegap

import (
	"fmt"
	"net/http"
	"time"
)

// DefaultGapEvent is the event name Handler uses to signal a gap when
// Options.OnGap is not set. See "Gap detection" in the package README for
// the full protocol this implements.
const DefaultGapEvent = "ssegap-gap"

// DefaultHeartbeat is used when Options.Heartbeat is left at its zero
// value.
const DefaultHeartbeat = 15 * time.Second

// DefaultQueueCapacity is used when Options.QueueCapacity is left at its
// zero value.
const DefaultQueueCapacity = 256

// GapInfo is passed to Options.OnGap, and is also what the default gap
// signal (see DefaultGapEvent) sends as its data, JSON-encoded as
// {"lastEventId":"..."}.
type GapInfo struct {
	// LastEventID is exactly what the client sent in its Last-Event-ID
	// request header.
	LastEventID string
}

// Options configures a Handler. The zero value is a usable, if minimal,
// configuration: no replay or gap detection (Log is nil), a 15s heartbeat,
// a 256-frame per-connection queue that drops the oldest frame on
// overflow.
type Options struct {
	// Log backs Last-Event-ID replay and gap detection. Nil disables
	// both: every connection is treated as new regardless of any
	// Last-Event-ID header it sends.
	Log EventLog

	// Heartbeat is the interval between ": heartbeat" comment lines,
	// sent to keep idle proxies and load balancers from timing out the
	// connection. Zero uses DefaultHeartbeat; a negative value disables
	// heartbeats entirely.
	Heartbeat time.Duration

	// RetryMillis, when non-zero, is sent once as a "retry:" field
	// immediately after headers, telling the client how long to wait
	// before reconnecting after a drop.
	RetryMillis int

	// QueueCapacity bounds the number of not-yet-written frames queued
	// per connection before OverflowPolicy applies. Zero uses
	// DefaultQueueCapacity.
	QueueCapacity int

	// OverflowPolicy chooses what happens when QueueCapacity is
	// reached for a connection that isn't draining fast enough. The
	// zero value is DropOldest.
	OverflowPolicy OverflowPolicy

	// GapEvent overrides DefaultGapEvent, the event name used for the
	// automatic gap signal (only relevant when OnGap is nil).
	GapEvent string

	// OnGap, if set, is called synchronously right after connect,
	// before any replay, whenever the client's Last-Event-ID is one
	// Log.Since could not honestly answer for. It replaces the
	// automatic gap signal entirely — nothing is sent to the client
	// unless this function sends it. Use this to fold the gap into
	// your own event vocabulary, log it, or trigger an
	// application-level resync instead of (or in addition to) telling
	// the client over SSE.
	//
	// When OnGap is nil and Log is non-nil, Handler sends
	// GapEvent/DefaultGapEvent automatically with a JSON GapInfo
	// payload, so gap detection works without any extra wiring.
	OnGap func(s *Session, info GapInfo)

	// OnConnect, if set, is called once per connection, after headers
	// are written and any replay/gap handling has completed, with the
	// Session the application uses to push events and detect
	// disconnect. This is the usual place to register a pub/sub
	// subscription and wire Session.Done to its teardown.
	OnConnect func(s *Session)
}

// Handler is an http.Handler that serves one SSE connection per request.
// Construct it with NewHandler and mount it at whatever path your clients'
// EventSource connects to; a single Handler is safe for concurrent use by
// any number of simultaneous connections.
type Handler struct {
	opts Options
}

// NewHandler builds a Handler from opts.
func NewHandler(opts Options) *Handler {
	if opts.Heartbeat == 0 {
		opts.Heartbeat = DefaultHeartbeat
	} else if opts.Heartbeat < 0 {
		opts.Heartbeat = 0
	}
	if opts.QueueCapacity <= 0 {
		opts.QueueCapacity = DefaultQueueCapacity
	}
	if opts.GapEvent == "" {
		opts.GapEvent = DefaultGapEvent
	}
	return &Handler{opts: opts}
}

// ServeHTTP implements http.Handler. It writes SSE response headers,
// performs Last-Event-ID replay or gap signaling if a Log is configured
// and the client sent that header, calls OnConnect, and then blocks —
// running this connection's single writer goroutine — until the client
// disconnects, the request context is canceled, or the session is closed.
// It never returns before that writer goroutine has fully exited.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	header := w.Header()
	header.Set("Content-Type", "text/event-stream; charset=utf-8")
	header.Set("Cache-Control", "no-cache")
	header.Set("Connection", "keep-alive")
	// Nginx and some other reverse proxies buffer proxied responses by
	// default, which delays every frame until the buffer fills — this
	// header tells them not to for this response. It's a no-op (and
	// harmless) everywhere else.
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	session := newSession(r, w, flusher, h.opts.Log, h.opts.QueueCapacity, h.opts.OverflowPolicy)

	if h.opts.RetryMillis > 0 {
		session.enqueue(appendRetry(nil, time.Duration(h.opts.RetryMillis)*time.Millisecond))
	}

	if lastID := r.Header.Get("Last-Event-ID"); lastID != "" && h.opts.Log != nil {
		h.replay(session, lastID)
	}

	if h.opts.OnConnect != nil {
		h.opts.OnConnect(session)
	}

	session.run(r.Context(), h.opts.Heartbeat)
}

func (h *Handler) replay(session *Session, lastID string) {
	events, ok := h.opts.Log.Since(lastID)
	if !ok {
		info := GapInfo{LastEventID: lastID}
		if h.opts.OnGap != nil {
			h.opts.OnGap(session, info)
			return
		}
		// enqueue, not Send: the gap signal is a protocol control frame,
		// not an application event, and must not consume an id from
		// (or be replayable out of) the event log.
		session.enqueue(appendMessage(nil, Message{
			Event: h.opts.GapEvent,
			Data:  fmt.Sprintf(`{"lastEventId":%q}`, info.LastEventID),
		}))
		return
	}
	for _, e := range events {
		session.enqueue(appendMessage(nil, e)) // already has its log-assigned ID; do not re-append to the log
	}
}
