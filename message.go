package ssegap

import (
	"strconv"
	"strings"
	"time"
)

// Message is a single Server-Sent Event. ID and Event are optional; Data is
// typically not empty, though the WHATWG spec permits an event with no
// data field at all (an empty Message dispatches a blank "data:" line, per
// spec — construct one explicitly with Data set to "" if that is what you
// want).
//
// Data is a plain string. Callers who need structured payloads encode them
// themselves (encoding/json and similar) before assigning Data; ssegap does
// not guess at encoding on the caller's behalf.
type Message struct {
	// ID becomes the event's "id:" field and the value a client echoes
	// back in a future Last-Event-ID header. When a Handler has an
	// EventLog configured, Send leaves ID empty and lets the log assign
	// one, so the id on the wire always matches what the log can later
	// answer for.
	ID string

	// Event becomes the "event:" field. Empty means the event has no
	// name, which EventSource delivers to "message" listeners.
	Event string

	// Data becomes one or more "data:" lines. A multi-line string is
	// split on CRLF, CR, or LF (per the WHATWG spec's line-splitting
	// rule) and each line gets its own "data:" prefix.
	Data string

	// Retry, when non-zero, becomes this event's "retry:" field,
	// instructing the client to change its reconnection delay. Most
	// callers set a connection-wide retry via Options.RetryMillis
	// instead; per-message Retry is for changing it mid-stream.
	Retry time.Duration
}

// splitDataLines splits s on the three line terminators the WHATWG SSE
// spec recognizes: CRLF, bare CR, and bare LF.
func splitDataLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return strings.Split(s, "\n")
}

// sanitizeField strips characters that would corrupt single-line fields
// (id, event) if a caller's value happens to contain a line break. The
// SSE wire format has no escaping mechanism for these fields, so the only
// safe options are strip or reject; ssegap strips, documented here rather
// than failing a Send call over a cosmetic detail.
func sanitizeField(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}

// appendMessage renders msg as SSE wire bytes (field lines followed by the
// blank-line dispatch terminator) and appends them to buf, returning the
// grown slice.
func appendMessage(buf []byte, msg Message) []byte {
	if msg.ID != "" {
		buf = append(buf, "id: "...)
		buf = append(buf, sanitizeField(msg.ID)...)
		buf = append(buf, '\n')
	}
	if msg.Event != "" {
		buf = append(buf, "event: "...)
		buf = append(buf, sanitizeField(msg.Event)...)
		buf = append(buf, '\n')
	}
	if msg.Retry > 0 {
		buf = append(buf, "retry: "...)
		buf = strconv.AppendInt(buf, msg.Retry.Milliseconds(), 10)
		buf = append(buf, '\n')
	}
	for _, line := range splitDataLines(msg.Data) {
		buf = append(buf, "data: "...)
		buf = append(buf, line...)
		buf = append(buf, '\n')
	}
	buf = append(buf, '\n')
	return buf
}

// appendComment renders text as one or more ": "-prefixed comment lines
// followed by the blank-line terminator. Comments are invisible to
// EventSource's event API but still count as traffic, which is what makes
// them useful as heartbeats.
func appendComment(buf []byte, text string) []byte {
	for _, line := range splitDataLines(text) {
		buf = append(buf, ": "...)
		buf = append(buf, line...)
		buf = append(buf, '\n')
	}
	buf = append(buf, '\n')
	return buf
}

// appendRetry renders a standalone "retry:" field, used to set the
// reconnection delay once at connect time.
func appendRetry(buf []byte, d time.Duration) []byte {
	buf = append(buf, "retry: "...)
	buf = strconv.AppendInt(buf, d.Milliseconds(), 10)
	buf = append(buf, '\n', '\n')
	return buf
}
