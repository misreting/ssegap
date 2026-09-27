# ssegap

Server-Sent Events for `net/http`, built around the one thing almost every
SSE implementation — Go's included — treats as optional: telling a
reconnecting client the truth when the server can't replay what it missed.

```
pushing 20000 events (~2048 bytes each) to a client that never reads

naive (unbounded queue)   heap grew +40450 KB
bounded (ssegap, cap 256) heap grew +1144 KB
```

That's the real output of `go run ./examples/backpressure` — see
[Evidence](#evidence).

## The problem

Go's standard library has no SSE support at all. Of the two libraries that
fill the gap:

- [`r3labs/sse`](https://github.com/r3labs/sse) is the more popular one
  (~1k GitHub stars) but its last commit was in January 2023 — it isn't
  going to gain gap detection, or anything else.
- [`tmaxmax/go-sse`](https://github.com/tmaxmax/go-sse) (~530 stars) is
  actively maintained (releases through 2023, 2024, and May 2025) and has
  a `Replayer` abstraction with real `Last-Event-ID` support. For plain
  broadcast — push events, replay what you can, don't worry about the
  rest — it's the right choice, and better maintained than this package.

But go-sse's replay is silent about what it can't do. Its two built-in
replayers (`FiniteReplayer`, capped by count; `ValidReplayer`, capped by
TTL) both resolve an unknown or expired `Last-Event-ID` the same way —
confirmed directly in `replay.go`:

```go
func (f *FiniteReplayer) Replay(subscription Subscription) error {
	i := findIDInQueue(&f.buf, subscription.LastEventID, f.currentID != nil)
	if i < 0 {
		return nil // <-- no error, no event, nothing. Replay just... didn't.
	}
	...
```

`findIDInQueue` returns `-1` for an id that was evicted from the buffer
and for one that was never issued — the two cases are indistinguishable to
the caller, and either way `Replay` returns `nil`: not an error, not a
partial result flagged as partial, nothing written to the client. A
reconnecting `EventSource` gets an open connection and silence, and has no
way to know it isn't caught up. That's the exact failure mode this package
exists to close: **silent resumption from a hole is data loss that looks
like success.**

(r3labs/sse's replay story is simpler still — there isn't one; it has no
`Last-Event-ID` handling at all.)

## Why the obvious fixes don't work

- **"Just keep more history."** You can always construct a gap bigger than
  your buffer — a laptop closed over a long weekend, a consumer down for
  maintenance. The question isn't "how much do we retain," it's "what do
  we do when retention isn't enough," and "nothing" is the wrong answer
  regardless of how the number is tuned.

- **"Slow clients are rare."** A phone that locks its screen, a laptop
  that sleeps mid-response, a proxy that briefly stops draining — none of
  these are exotic, and in Go specifically, `http.ResponseWriter`'s
  `Flush` *blocks* when the client stops reading. Call that directly from
  whatever goroutine is publishing events (an order handler, a pub/sub
  callback) and one slow subscriber stalls every publish. The fix nearly
  everyone reaches for — decouple with a per-connection queue — just
  relocates the problem to memory: an unbounded queue behind a stalled
  reader grows forever. The benchmark above measures exactly that
  relocation and what capping the queue does instead.

## Install

```sh
go get github.com/agentscope-ai-java/ssegap
```

Go >= 1.21 (developed and tested against 1.27). Zero runtime dependencies —
`go.uber.org/goleak` is a test-only import, used to prove the no-leaked-
goroutines claim below; it never ships in your binary.

## Use

```go
package main

import (
	"log"
	"net/http"

	"github.com/agentscope-ai-java/ssegap"
)

func main() {
	eventLog := ssegap.NewMemoryEventLog(1000) // last 1000 events, in memory

	handler := ssegap.NewHandler(ssegap.Options{
		Log: eventLog,
		OnConnect: func(s *ssegap.Session) {
			unsubscribe := bus.Subscribe(func(order Order) {
				s.Send(ssegap.Message{Event: "order", Data: order.JSON()})
			})
			go func() {
				<-s.Done() // client disconnected, context canceled, or Close was called
				unsubscribe()
			}()
		},
		// Optional: react to a gap yourself instead of the automatic
		// wire signal (see "Gap detection" below).
		OnGap: func(s *ssegap.Session, info ssegap.GapInfo) {
			s.Send(ssegap.Message{Event: "resync-required", Data: info.LastEventID})
		},
	})

	http.Handle("/events", handler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

`NewHandler` builds an `http.Handler`; mount it wherever your `EventSource`
clients connect. It writes SSE headers immediately, replays anything a
`Last-Event-ID` request header says was missed (if `Log` is configured and
the header is present), and otherwise blocks running that connection's
writer for as long as the client stays connected.

```go
type Session struct { /* ... */ }

func (s *Session) Send(msg Message)      // enqueue an event; never blocks on the client
func (s *Session) Comment(text string)   // raw ": text" line — ignored by EventSource, still traffic
func (s *Session) Done() <-chan struct{} // closed when the connection ends
func (s *Session) Close()                // end the connection from the server side
func (s *Session) Request() *http.Request
```

`Message.Data` is a plain string; encode structured payloads yourself
(`encoding/json` or whatever fits) before assigning it. Every line of a
multi-line `Data` gets its own `data:` prefix, per spec. When `Log` is
configured, `Send` persists the event first (assigning `Message.ID` if it
was empty) so the id on the wire always matches what the log can later
answer for.

## Options

| Option | Default | What it controls |
| --- | --- | --- |
| `Log` | `nil` | Backs `Last-Event-ID` replay and gap detection. `nil` disables both — every connection is treated as new. |
| `Heartbeat` | 15s | Interval between `: heartbeat` comment lines, to defeat idle proxy/load-balancer timeouts. Negative disables. |
| `RetryMillis` | unset | Sent once as `retry:` right after headers, telling the client its reconnect delay. |
| `QueueCapacity` | 256 | Frames queued per connection, waiting to be written, before `OverflowPolicy` applies. |
| `OverflowPolicy` | `DropOldest` | `DropOldest` \| `DropNewest` \| `Disconnect` — see below. |
| `GapEvent` | `"ssegap-gap"` | Event name for the automatic gap signal (ignored if `OnGap` is set). |
| `OnGap` | `nil` | Called synchronously, before any replay, when the client's `Last-Event-ID` can't be honestly answered. Replaces the automatic signal entirely when set. |
| `OnConnect` | `nil` | Called once per connection with its `*Session`, after headers and any replay/gap handling. |

### `MemoryEventLog`

```go
ssegap.NewMemoryEventLog(1000) // capacity; default 1000 if <= 0
```

A bounded ring buffer: the last `capacity` events, in memory, per process.
Cheap, zero setup, and enough for a single-instance server. It is not
shared across processes — behind a load balancer with more than one
instance, or across a restart, a client can reconnect to a different
instance than the one holding its history. For that, implement the
`EventLog` interface (`Append`, `Since`) against something shared: Redis (a
capped stream or sorted set), or a database table with a monotonic,
indexed id column.

### Gap detection

There is no wire-level standard for telling an `EventSource` "you missed
things I can no longer give you" — SSE was never designed with that
signal in mind. This package documents one as an explicit convention, not
a standard:

**The contract, server side:** an `EventLog.Since(lastID)` returns
`ok == false` when it cannot honestly answer — the id was evicted, never
issued, or unparseable. Returning `ok == true` with an incomplete slice is
the one thing an `EventLog` must never do.

**What `Handler` does with that:** when `ok == false`, and no `OnGap` hook
is set, it sends one event named `ssegap-gap` (override via `GapEvent`)
with a JSON body — `{"lastEventId":"<what the client sent>"}` — before
anything else. Setting `OnGap` replaces this entirely: you decide what (if
anything) reaches the client, e.g. folding it into your own event
vocabulary instead of a dedicated one.

**What the client has to do:** listen for it, the same way it would any
named SSE event:

```js
const es = new EventSource('/events');
es.addEventListener('ssegap-gap', (e) => {
  const { lastEventId } = JSON.parse(e.data);
  // refetch a snapshot, flag stale UI state, log it — your call.
  // The one thing not to do is nothing: an unhandled gap means the
  // client believes it's caught up when it isn't.
});
```

Be clear-eyed about what this convention is: an application-level event
name and payload shape, agreed between this server and your client code —
not something the SSE/EventSource spec or any browser API understands
natively. A generic SSE consumer that only watches for unnamed `message`
events will never see it, exactly like any other custom named event.

### `OverflowPolicy`

Once `QueueCapacity` is reached for a connection whose writer can't keep
up:

- **`DropOldest`** — keep the most recent frames, discard the rest. Good
  for live dashboards and tickers, where current state matters more than
  a complete history.
- **`DropNewest`** — keep what's already queued, refuse new frames until
  the writer catches up. Good when the order and completeness of what's
  already in flight matters more than freshness.
- **`Disconnect`** — close the connection. Good when a full queue means
  the client is far enough behind that continuing would be misleading —
  better to have it reconnect (and, with a `Log` configured, get a clean
  replay or an honest gap signal) than keep silently falling further
  behind.

None of these lose data quietly: `Session.Send` persists to `Log` (if
configured) *before* the frame is queued, so `DropOldest`/`DropNewest` only
ever discard bytes that were never handed to the socket — the events
themselves are still in the log, and a reconnect can still replay them or
get a proper gap signal. `Disconnect` is explicit by nature.

## Backpressure and goroutine lifetime

Every connection gets exactly one writer goroutine. `Session.Send` and
`Session.Comment` never touch the socket directly — they push a frame onto
a bounded queue and return; the writer goroutine is the only thing that
ever calls `Write`/`Flush`, so it's the only thing that can ever block on
a stalled client. `Handler.ServeHTTP` doesn't return until that goroutine
has exited, whether the connection ended because the client disconnected,
the request context was canceled, or the server called `Session.Close`.

That "doesn't return until the goroutine has exited" claim is checked by
`TestNoGoroutineLeakAfterDisconnect`, which opens and cleanly tears down
ten connections (heartbeats included, to exercise that ticker's teardown
path too) under
[`go.uber.org/goleak`](https://github.com/uber-go/goleak)'s
`VerifyNone` — the standard tool for this in Go, used here as the
test-only dependency it is rather than reimplemented by hand.

## Evidence

`go run ./examples/backpressure` builds the same architecture two ways —
an unbounded per-connection queue (the pattern most hand-rolled SSE
servers reach for once they realize they can't call `Flush` directly from
a publisher) and `ssegap`'s bounded one — and pushes 20,000 ~2KB events
into each against a client that opens the connection and never reads:

```
$ go run ./examples/backpressure
pushing 20000 events (~2048 bytes each) to a client that never reads

naive (unbounded queue)   heap grew +40450 KB
bounded (ssegap, cap 256) heap grew +1144 KB
```

The bounded run's residual growth is close to what `QueueCapacity: 256`
holding ~2KB frames predicts (~512 KB) plus normal allocator overhead —
not zero, because the queue itself is real memory, but capped regardless
of how long the client stays stalled or how many more events get
published. The naive run's growth is unbounded in the same proportion:
double the event count and its number roughly doubles, while the bounded
number does not move.

## What it does not do

- **No channels or rooms.** Broadcasting to many sessions is your loop
  over whatever sessions you're tracking; `Handler` serves one connection
  at a time by design.
- **No built-in `EventLog` beyond memory.** A Redis- or database-backed
  log is an interface to implement against your own storage, not
  something this package ships.
- **No structured-data convenience.** `Message.Data` is a string; there is
  no automatic JSON marshaling. This is a deliberate departure from some
  other SSE libraries' `any`-typed data fields — one fewer place for a
  caller's data to be silently reinterpreted.
- **HTTP/1.1 semantics, same as `EventSource` itself.** No WebSocket
  fallback. The `Connection: keep-alive` header this package sets is
  meaningless (and stripped) on HTTP/2; everything else works the same
  either way.
- **Not a client.** This is the server half; pair it with the browser's
  built-in `EventSource`, or any SSE client that already speaks
  `Last-Event-ID` on reconnect, plus whatever `ssegap-gap`-equivalent
  listener your application needs.
- **The gap-detection signal is a convention, not a standard.** Said twice
  because it matters: nothing in the SSE spec or any `EventSource`
  implementation understands `ssegap-gap` natively. It's an ordinary named
  event your client code has to know to listen for.

## Develop

```sh
go build ./...
go vet ./...
go test -race ./...
gofmt -l .        # must print nothing
```

## License

MIT
