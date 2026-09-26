package ssegap

import "testing"

func TestMemoryEventLog_AppendAssignsMonotonicIDs(t *testing.T) {
	log := NewMemoryEventLog(10)
	m1 := log.Append(Message{Data: "one"})
	m2 := log.Append(Message{Data: "two"})
	if m1.ID != "1" || m2.ID != "2" {
		t.Fatalf("got ids %q, %q; want 1, 2", m1.ID, m2.ID)
	}
}

func TestMemoryEventLog_ReplayFromKnownID(t *testing.T) {
	log := NewMemoryEventLog(10)
	log.Append(Message{Data: "a"})
	log.Append(Message{Data: "b"})
	log.Append(Message{Data: "c"})

	events, ok := log.Since("1")
	if !ok {
		t.Fatalf("Since(1) reported a gap, want a clean replay")
	}
	if len(events) != 2 || events[0].Data != "b" || events[1].Data != "c" {
		t.Fatalf("Since(1) = %+v, want [b c]", events)
	}
}

func TestMemoryEventLog_ReplayFromLastKnownIDIsEmpty(t *testing.T) {
	log := NewMemoryEventLog(10)
	log.Append(Message{Data: "a"})
	log.Append(Message{Data: "b"})

	events, ok := log.Since("2")
	if !ok {
		t.Fatalf("Since(2) reported a gap, want a clean (empty) replay")
	}
	if len(events) != 0 {
		t.Fatalf("Since(2) = %+v, want no events (client already caught up)", events)
	}
}

func TestMemoryEventLog_GapWhenEvicted(t *testing.T) {
	log := NewMemoryEventLog(3) // ring buffer capacity 3
	for i := 0; i < 6; i++ {
		log.Append(Message{Data: "event"})
	}
	// ids 1..6 assigned; only 4,5,6 retained (1,2,3 evicted).

	if _, ok := log.Since("2"); ok {
		t.Fatalf("Since(2) reported ok, want a gap: id 2 was evicted (3 was too, between 2 and the oldest retained id 4)")
	}
	// The boundary case: "3" is the id immediately before the oldest
	// retained event (4), so nothing between them was lost.
	events, ok := log.Since("3")
	if !ok {
		t.Fatalf("Since(3) reported a gap, want a clean replay (3 is the eviction boundary, nothing after it was lost)")
	}
	if len(events) != 3 {
		t.Fatalf("Since(3) = %d events, want 3 (ids 4,5,6)", len(events))
	}
}

func TestMemoryEventLog_GapWhenNeverIssued(t *testing.T) {
	log := NewMemoryEventLog(10)
	log.Append(Message{Data: "a"})

	if _, ok := log.Since("999"); ok {
		t.Fatalf("Since(999) reported ok, want a gap: id 999 was never assigned")
	}
}

func TestMemoryEventLog_GapWhenUnparseable(t *testing.T) {
	log := NewMemoryEventLog(10)
	log.Append(Message{Data: "a"})

	if _, ok := log.Since("not-a-number"); ok {
		t.Fatalf("Since(garbage) reported ok, want a gap")
	}
}

func TestMemoryEventLog_EmptyLogNoGapForZero(t *testing.T) {
	log := NewMemoryEventLog(10)
	events, ok := log.Since("0")
	if !ok {
		t.Fatalf("Since(0) on an empty log reported a gap, want a clean empty replay")
	}
	if len(events) != 0 {
		t.Fatalf("Since(0) on an empty log = %+v, want none", events)
	}
}

func TestMemoryEventLog_WithIDs_ExactMatch(t *testing.T) {
	log := NewMemoryEventLogWithIDs(10)
	log.Append(Message{ID: "a", Data: "1"})
	log.Append(Message{ID: "b", Data: "2"})
	log.Append(Message{ID: "c", Data: "3"})

	events, ok := log.Since("a")
	if !ok || len(events) != 2 {
		t.Fatalf("Since(a) = %+v, ok=%v; want [b c], ok=true", events, ok)
	}

	if _, ok := log.Since("nonexistent"); ok {
		t.Fatalf("Since(nonexistent) reported ok, want a gap")
	}
}

func TestMemoryEventLog_RingBufferWraps(t *testing.T) {
	log := NewMemoryEventLog(2)
	log.Append(Message{Data: "a"})
	log.Append(Message{Data: "b"})
	log.Append(Message{Data: "c"}) // evicts "a"
	log.Append(Message{Data: "d"}) // evicts "b"

	events, ok := log.Since("2")
	if !ok {
		t.Fatalf("Since(2) reported a gap, want a clean replay of c,d")
	}
	if len(events) != 2 || events[0].Data != "c" || events[1].Data != "d" {
		t.Fatalf("Since(2) = %+v, want [c d]", events)
	}
}
