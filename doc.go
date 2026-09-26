// Package ssegap is a server-side Server-Sent Events (SSE) implementation
// for net/http with one feature almost no Go SSE library has: an explicit,
// documented signal for the client when a reconnect asks for events the
// server can no longer supply.
//
// SSE's Last-Event-ID mechanism lets a reconnecting client tell the server
// where it left off, and lets the server replay whatever was missed. Nearly
// every implementation — including the actively maintained ones — replays
// what it still has and says nothing when it doesn't have enough: the
// client believes it caught up when it didn't. ssegap's [EventLog] makes
// "I don't know what you missed" a first-class return value, and
// [Handler] turns that into a wire-level signal a client can act on. See
// the "Gap detection" section of the package README for the exact
// protocol and its limits.
//
// A [Handler] also owns backpressure: each connection gets a single writer
// goroutine and a bounded queue, so a stalled client (an asleep phone, a
// backgrounded tab, a proxy that stopped draining) costs a fixed amount of
// memory instead of an unbounded amount.
package ssegap
