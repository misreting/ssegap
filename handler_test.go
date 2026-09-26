package ssegap

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// rawGet performs a GET against the server with an optional Last-Event-ID
// header and returns the raw response body, headers, and a cancel func
// the caller should invoke to end the request (which is how a real
// EventSource "disconnects": the client goes away, the request context is
// canceled).
func rawGet(t *testing.T, url, lastEventID string) (*http.Response, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("Do: %v", err)
	}
	return resp, cancel
}

// readFrames reads n complete "field lines + blank line" frames from r,
// returning each frame's raw text (fields joined by "\n", no trailing
// blank line). It fails the test if the stream ends first.
func readFrames(t *testing.T, r *bufio.Reader, n int) []string {
	t.Helper()
	var frames []string
	var cur []string
	for len(frames) < n {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("reading frames: got %d of %d, err: %v", len(frames), n, err)
		}
		line = strings.TrimRight(line, "\n")
		if line == "" {
			if len(cur) > 0 {
				frames = append(frames, strings.Join(cur, "\n"))
				cur = nil
			}
			continue
		}
		cur = append(cur, line)
	}
	return frames
}

func TestServeHTTP_HeadersAndFraming(t *testing.T) {
	h := NewHandler(Options{Heartbeat: -1})
	var sess *Session
	connected := make(chan struct{})
	h.opts.OnConnect = func(s *Session) {
		sess = s
		s.Send(Message{Event: "greeting", Data: "hello\nworld"})
		close(connected)
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, cancel := rawGet(t, srv.URL, "")
	defer cancel()
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q, want text/event-stream prefix", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want no-cache", cc)
	}
	if xa := resp.Header.Get("X-Accel-Buffering"); xa != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", xa)
	}

	<-connected
	_ = sess

	br := bufio.NewReader(resp.Body)
	frames := readFrames(t, br, 1)
	want := "event: greeting\ndata: hello\ndata: world"
	if frames[0] != want {
		t.Errorf("frame = %q, want %q", frames[0], want)
	}
}

func TestServeHTTP_RetryFieldSentOnce(t *testing.T) {
	h := NewHandler(Options{Heartbeat: -1, RetryMillis: 3000})
	h.opts.OnConnect = func(s *Session) {
		s.Send(Message{Data: "x"})
	}
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, cancel := rawGet(t, srv.URL, "")
	defer cancel()
	defer resp.Body.Close()

	br := bufio.NewReader(resp.Body)
	frames := readFrames(t, br, 2)
	if frames[0] != "retry: 3000" {
		t.Errorf("first frame = %q, want retry: 3000", frames[0])
	}
	if frames[1] != "data: x" {
		t.Errorf("second frame = %q, want data: x", frames[1])
	}
}

func TestReplayAfterReconnect(t *testing.T) {
	log := NewMemoryEventLog(100)
	connected := make(chan *Session, 4)
	h := NewHandler(Options{
		Heartbeat: -1,
		Log:       log,
		OnConnect: func(s *Session) {
			connected <- s
		},
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	// First connection: publish 3 events, read them, then disconnect
	// having only "seen" the first two (simulating a drop mid-stream).
	// cancel1/resp1.Body.Close are deferred immediately (rather than
	// called procedurally further down) so the connection is always
	// torn down even if an assertion below fails the test early —
	// otherwise httptest.Server.Close, deferred above, would block
	// forever waiting for a connection nothing ever closed.
	resp1, cancel1 := rawGet(t, srv.URL, "")
	defer cancel1()
	defer resp1.Body.Close()
	br1 := bufio.NewReader(resp1.Body)

	s := <-connected
	s.Send(Message{Data: "one"})
	s.Send(Message{Data: "two"})
	s.Send(Message{Data: "three"})
	frames := readFrames(t, br1, 3)
	if frames[1] != "id: 2\ndata: two" {
		t.Fatalf("frame[1] = %q, want id: 2\\ndata: two", frames[1])
	}
	lastSeen := "2" // pretend the connection dropped after event 2
	cancel1()
	resp1.Body.Close()

	// Reconnect with Last-Event-ID: 2. The server should replay event 3
	// (and only event 3) before any new events.
	resp2, cancel2 := rawGet(t, srv.URL, lastSeen)
	defer cancel2()
	defer resp2.Body.Close()
	br2 := bufio.NewReader(resp2.Body)
	replayed := readFrames(t, br2, 1)
	want := "id: 3\ndata: three"
	if replayed[0] != want {
		t.Fatalf("replay = %q, want %q", replayed[0], want)
	}
}

func TestGapDetection_DefaultSignal(t *testing.T) {
	log := NewMemoryEventLog(2) // tiny ring: easy to evict
	h := NewHandler(Options{Heartbeat: -1, Log: log})
	srv := httptest.NewServer(h)
	defer srv.Close()

	for i := 0; i < 5; i++ {
		log.Append(Message{Data: fmt.Sprintf("event-%d", i)})
	}
	// ids 1..5 assigned, capacity 2, so only ids 4 and 5 remain: id "1"
	// is long gone.

	resp, cancel := rawGet(t, srv.URL, "1")
	defer cancel()
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	frames := readFrames(t, br, 1)

	if !strings.HasPrefix(frames[0], "event: "+DefaultGapEvent+"\n") {
		t.Fatalf("frame = %q, want it to start with the default gap event", frames[0])
	}
	if !strings.Contains(frames[0], `"lastEventId":"1"`) {
		t.Fatalf("frame = %q, want it to carry the client's Last-Event-ID", frames[0])
	}
}

func TestGapDetection_CustomOnGapHook(t *testing.T) {
	log := NewMemoryEventLog(2)
	var gotID string
	called := make(chan struct{})
	h := NewHandler(Options{
		Heartbeat: -1,
		Log:       log,
		OnGap: func(s *Session, info GapInfo) {
			gotID = info.LastEventID
			s.Send(Message{Event: "resync", Data: "please refetch"})
			close(called)
		},
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	for i := 0; i < 5; i++ {
		log.Append(Message{Data: "x"})
	}

	resp, cancel := rawGet(t, srv.URL, "1")
	defer cancel()
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	frames := readFrames(t, br, 1)
	<-called

	if gotID != "1" {
		t.Errorf("OnGap got LastEventID %q, want 1", gotID)
	}
	// s.Send goes through the log like any other application event
	// (unlike the built-in default gap signal, which deliberately
	// bypasses it — see the comment in Handler.replay), so it picks up
	// a log-assigned id.
	if frames[0] != "id: 6\nevent: resync\ndata: please refetch" {
		t.Errorf("frame = %q, want the custom resync event with a log-assigned id", frames[0])
	}
}

func TestGapDetection_NoGapWhenFullyCaughtUp(t *testing.T) {
	log := NewMemoryEventLog(100)
	h := NewHandler(Options{Heartbeat: -1, Log: log})
	srv := httptest.NewServer(h)
	defer srv.Close()

	m := log.Append(Message{Data: "only event"})

	// Reconnect claiming to have already seen the only event in the
	// log: no gap, and nothing to replay, but the connection should
	// stay open and deliver whatever comes next.
	resp, cancel := rawGet(t, srv.URL, m.ID)
	defer cancel()
	defer resp.Body.Close()

	// Nothing has been sent yet on this connection and there is no gap:
	// the read below should time out, not return a frame. It must not
	// call any *testing.T method from this goroutine (only the result
	// channel does that, back on the test goroutine): the deferred
	// cancel()/resp.Body.Close() above are what eventually unblock this
	// read, at the very end of the test, well after the timeout case
	// below has already been taken.
	br := bufio.NewReader(resp.Body)
	result := make(chan error, 1)
	go func() {
		_, err := br.ReadString('\n')
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("received a frame with nothing published and no gap, want none")
		}
	case <-time.After(150 * time.Millisecond):
		// expected: connection open, silent, no gap signal
	}
}

func TestBackpressure_DropOldestBoundsQueue(t *testing.T) {
	connected := make(chan *Session, 1)
	h := NewHandler(Options{
		Heartbeat:      -1,
		QueueCapacity:  8,
		OverflowPolicy: DropOldest,
		OnConnect: func(s *Session) {
			connected <- s
		},
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Connect but never read the body: a stalled client. Use a raw TCP
	// dial-free client with a tiny response body isn't easy to stall
	// portably, so instead we drive backpressure directly at the
	// Session/outbox level, which is where this package actually
	// enforces the bound (the HTTP transport's own buffering makes a
	// stall hard to reproduce deterministically in-process). See
	// TestOutbox_DropOldest for the same guarantee at the unit level.
	resp, cancel := rawGet(t, srv.URL, "")
	defer cancel()
	defer resp.Body.Close()

	s := <-connected
	for i := 0; i < 1000; i++ {
		s.Send(Message{Data: fmt.Sprintf("event-%d", i)})
	}
	s.outbox.mu.Lock()
	n := len(s.outbox.queue)
	s.outbox.mu.Unlock()
	if n > 8 {
		t.Fatalf("outbox queue grew to %d, want <= capacity (8)", n)
	}
}

func TestOutbox_DropOldestKeepsMostRecent(t *testing.T) {
	o := newOutbox(3, DropOldest)
	for i := 0; i < 5; i++ {
		o.push([]byte(strconv.Itoa(i)))
	}
	got := o.drain()
	if len(got) != 3 {
		t.Fatalf("queue length = %d, want 3", len(got))
	}
	want := []string{"2", "3", "4"}
	for i, w := range want {
		if string(got[i]) != w {
			t.Errorf("frame[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestOutbox_DropNewestKeepsQueuedOrder(t *testing.T) {
	o := newOutbox(3, DropNewest)
	for i := 0; i < 5; i++ {
		o.push([]byte(strconv.Itoa(i)))
	}
	got := o.drain()
	want := []string{"0", "1", "2"}
	if len(got) != 3 {
		t.Fatalf("queue length = %d, want 3", len(got))
	}
	for i, w := range want {
		if string(got[i]) != w {
			t.Errorf("frame[%d] = %q, want %q", i, got[i], w)
		}
	}
}

func TestOutbox_DisconnectTripsOnce(t *testing.T) {
	o := newOutbox(2, Disconnect)
	o.push([]byte("a"))
	o.push([]byte("b"))
	if force := o.push([]byte("c")); !force {
		t.Fatalf("first overflow push should report forceClose=true")
	}
	if force := o.push([]byte("d")); force {
		t.Fatalf("second overflow push should not report forceClose again")
	}
}

func TestBackpressure_StalledRealClientBoundsMemoryAndFramesQueue(t *testing.T) {
	// A genuinely stalled client: dial the server, send the request
	// line ourselves, then never read the response body. The
	// http.Flusher-backed writer goroutine will eventually block once
	// the kernel's own socket buffers fill, at which point further
	// Sends must still return promptly (never block the caller) and
	// the queue must stay bounded via the overflow policy.
	const capacity = 16
	var sess atomic.Pointer[Session]
	h := NewHandler(Options{
		Heartbeat:      -1,
		QueueCapacity:  capacity,
		OverflowPolicy: DropOldest,
		OnConnect: func(s *Session) {
			sess.Store(s)
		},
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if sess.Load() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	s := sess.Load()
	if s == nil {
		t.Fatal("connection never reached OnConnect")
	}

	// Never read conn's response. Send far more than the queue and any
	// kernel socket buffer can hold, from this goroutine: if Send
	// blocked on a stalled reader, this loop itself would hang and the
	// test would time out.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 200_000; i++ {
			s.Send(Message{Data: "0123456789"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Send blocked on a stalled client instead of applying the overflow policy")
	}

	s.outbox.mu.Lock()
	n := len(s.outbox.queue)
	s.outbox.mu.Unlock()
	if n > capacity {
		t.Fatalf("outbox queue grew to %d, want <= capacity (%d)", n, capacity)
	}
}

func TestNoGoroutineLeakAfterDisconnect(t *testing.T) {
	defer goleak.VerifyNone(t)

	log := NewMemoryEventLog(10)
	connected := make(chan *Session, 20)
	h := NewHandler(Options{
		Heartbeat: 10 * time.Millisecond, // exercise the heartbeat ticker path too
		Log:       log,
		OnConnect: func(s *Session) {
			connected <- s
		},
	})
	srv := httptest.NewServer(h)
	defer srv.Close()

	for i := 0; i < 10; i++ {
		resp, cancel := rawGet(t, srv.URL, "")
		s := <-connected
		s.Send(Message{Data: "x"})
		// Give the writer goroutine a moment to actually write before
		// we yank the connection out from under it.
		buf := make([]byte, 64)
		_, _ = resp.Body.Read(buf)
		cancel()
		resp.Body.Close()
		// Wait for this connection's session to fully report closed,
		// which happens only after its writer goroutine has exited.
		select {
		case <-s.Done():
		case <-time.After(2 * time.Second):
			t.Fatalf("connection %d: session never reported done", i)
		}
	}

	// Give the server a moment to finish tearing down the underlying
	// net/http per-request goroutines (outside this package's control)
	// before goleak takes its snapshot.
	time.Sleep(50 * time.Millisecond)
}
