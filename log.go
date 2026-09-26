package ssegap

import (
	"strconv"
	"sync"
)

// EventLog is a pluggable, append-only store of published events, used to
// answer a reconnecting client's Last-Event-ID.
//
// # The gap-detection contract
//
// Since must return ok == false when it cannot honestly answer "what came
// after lastID" — because lastID has been evicted, or was never issued, or
// is unparseable garbage. Returning ok == true with a possibly-incomplete
// slice is the one thing an EventLog implementation must never do: a
// caller who gets ok == true trusts that events is everything since
// lastID, with nothing skipped in between.
//
// There is no wire-level standard for signaling this to a client — SSE and
// EventSource were never designed with it in mind. ssegap's [Handler]
// layers a convention on top (see the README's "Gap detection" section);
// EventLog implementations only need to report the gap accurately, not
// know how it gets communicated.
type EventLog interface {
	// Append stores msg and returns the stored copy. If msg.ID is
	// empty, Append assigns one (typically a monotonically increasing
	// counter formatted as a decimal string) and returns msg with ID
	// set, so the id on the wire always matches what Since can later
	// answer for.
	Append(msg Message) Message

	// Since returns every event stored after lastID, oldest first. It
	// returns ok == false when lastID is outside what the log can
	// answer for — evicted, unknown, or malformed — rather than
	// guessing. Since is never called with an empty lastID; a Handler
	// only consults the log when the client sent a Last-Event-ID.
	Since(lastID string) (events []Message, ok bool)
}

// MemoryEventLog is a bounded, in-process ring buffer implementation of
// EventLog: the last Capacity events, held in memory, gone on restart and
// not shared across instances. It is enough for a single-instance server
// and the natural default; behind a load balancer with more than one
// backend, or across a deploy, implement EventLog against something
// shared (Redis, a capped collection, a database table with a monotonic,
// indexed id column) so a client can reconnect to any instance and still
// get an honest answer.
//
// MemoryEventLog assigns its own ids (monotonically increasing decimal
// integers, starting at 1) when a [Message] arrives with ID unset, and
// uses that sequence internally to tell "evicted" apart from "never
// issued" apart from "still here". Supplying your own IDs on every
// message disables that bookkeeping — see [NewMemoryEventLogWithIDs] — and
// the caller becomes responsible for ids being both monotonic and
// comparable as unsigned decimal integers, or gap detection degenerates to
// exact-match lookup only.
type MemoryEventLog struct {
	mu      sync.Mutex
	buf     []logEntry
	head    int // index of the oldest retained entry
	count   int // number of entries currently retained
	nextSeq uint64
	autoID  bool
}

type logEntry struct {
	seq uint64
	msg Message
}

// NewMemoryEventLog creates a MemoryEventLog that retains the most recent
// capacity events and assigns their ids automatically. capacity <= 0 is
// treated as 1000.
func NewMemoryEventLog(capacity int) *MemoryEventLog {
	if capacity <= 0 {
		capacity = 1000
	}
	return &MemoryEventLog{
		buf:     make([]logEntry, capacity),
		nextSeq: 1,
		autoID:  true,
	}
}

// NewMemoryEventLogWithIDs creates a MemoryEventLog that retains the most
// recent capacity events but requires every appended [Message] to already
// carry a non-empty, unique ID chosen by the caller. Use this only when
// ids must come from elsewhere (e.g. a database row id); prefer
// [NewMemoryEventLog] otherwise, since caller-supplied ids that are not
// monotonically increasing decimal integers make eviction detection fall
// back to exact-match lookup, which cannot distinguish "evicted" from
// "never issued".
func NewMemoryEventLogWithIDs(capacity int) *MemoryEventLog {
	if capacity <= 0 {
		capacity = 1000
	}
	return &MemoryEventLog{
		buf: make([]logEntry, capacity),
	}
}

// Append implements EventLog.
func (l *MemoryEventLog) Append(msg Message) Message {
	l.mu.Lock()
	defer l.mu.Unlock()

	seq := l.nextSeq
	if l.autoID && msg.ID == "" {
		msg.ID = strconv.FormatUint(seq, 10)
	}
	l.nextSeq++

	tail := (l.head + l.count) % len(l.buf)
	l.buf[tail] = logEntry{seq: seq, msg: msg}
	if l.count == len(l.buf) {
		l.head = (l.head + 1) % len(l.buf) // overwritten the oldest slot
	} else {
		l.count++
	}
	return msg
}

// Since implements EventLog.
//
// When the log was built with NewMemoryEventLog (auto ids), lastID is
// parsed as the decimal sequence number it was assigned. If it is not
// parseable, or refers to a sequence the log never assigned, or to one
// that has since been evicted, Since returns ok == false. Otherwise it
// returns every retained event with a later sequence number.
//
// When the log was built with NewMemoryEventLogWithIDs, Since instead
// scans the retained events for an exact ID match; if lastID is not
// found among them, it returns ok == false — including when lastID was
// in fact evicted, since a plain ID string carries no ordering
// information to tell that apart from "never issued".
func (l *MemoryEventLog) Since(lastID string) (events []Message, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.autoID {
		return l.sinceAutoID(lastID)
	}
	return l.sinceExactID(lastID)
}

func (l *MemoryEventLog) sinceAutoID(lastID string) ([]Message, bool) {
	seq, err := strconv.ParseUint(lastID, 10, 64)
	if err != nil {
		return nil, false // not one of ours
	}

	lastAssigned := l.nextSeq - 1 // 0 if nothing has ever been appended
	if seq > lastAssigned {
		return nil, false // never issued (or from a previous, reset epoch)
	}

	firstRetained := l.nextSeq // empty log: nothing to evict from
	if l.count > 0 {
		firstRetained = l.buf[l.head].seq
	}
	if seq < firstRetained-1 {
		return nil, false // at least one event between seq and firstRetained was evicted
	}

	var out []Message
	for i := 0; i < l.count; i++ {
		e := l.buf[(l.head+i)%len(l.buf)]
		if e.seq > seq {
			out = append(out, e.msg)
		}
	}
	return out, true
}

func (l *MemoryEventLog) sinceExactID(lastID string) ([]Message, bool) {
	found := -1
	for i := 0; i < l.count; i++ {
		idx := (l.head + i) % len(l.buf)
		if l.buf[idx].msg.ID == lastID {
			found = i
			break
		}
	}
	if found < 0 {
		return nil, false
	}

	var out []Message
	for i := found + 1; i < l.count; i++ {
		idx := (l.head + i) % len(l.buf)
		out = append(out, l.buf[idx].msg)
	}
	return out, true
}
