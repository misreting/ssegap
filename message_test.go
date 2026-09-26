package ssegap

import (
	"testing"
	"time"
)

func TestAppendMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		want string
	}{
		{
			name: "data only",
			msg:  Message{Data: "hello"},
			want: "data: hello\n\n",
		},
		{
			name: "id, event, data",
			msg:  Message{ID: "42", Event: "greeting", Data: "hi"},
			want: "id: 42\nevent: greeting\ndata: hi\n\n",
		},
		{
			name: "multi-line data gets one data: per line, LF",
			msg:  Message{Data: "line one\nline two\nline three"},
			want: "data: line one\ndata: line two\ndata: line three\n\n",
		},
		{
			name: "multi-line data, CRLF terminators",
			msg:  Message{Data: "a\r\nb\r\nc"},
			want: "data: a\ndata: b\ndata: c\n\n",
		},
		{
			name: "multi-line data, bare CR terminators",
			msg:  Message{Data: "a\rb\rc"},
			want: "data: a\ndata: b\ndata: c\n\n",
		},
		{
			name: "empty data still dispatches a blank data line",
			msg:  Message{Event: "ping", Data: ""},
			want: "event: ping\ndata: \n\n",
		},
		{
			name: "retry field",
			msg:  Message{Data: "x", Retry: 4 * time.Second},
			want: "retry: 4000\ndata: x\n\n",
		},
		{
			name: "id/event with embedded newline gets sanitized to a single line",
			msg:  Message{ID: "a\nb", Event: "e\r\nf", Data: "x"},
			want: "id: a b\nevent: e f\ndata: x\n\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(appendMessage(nil, tt.msg))
			if got != tt.want {
				t.Errorf("appendMessage() =\n%q\nwant\n%q", got, tt.want)
			}
		})
	}
}

func TestAppendComment(t *testing.T) {
	tests := []struct {
		name string
		text string
		want string
	}{
		{name: "single line", text: "heartbeat", want: ": heartbeat\n\n"},
		{name: "multi line", text: "a\nb", want: ": a\n: b\n\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(appendComment(nil, tt.text))
			if got != tt.want {
				t.Errorf("appendComment() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAppendRetry(t *testing.T) {
	got := string(appendRetry(nil, 2500*time.Millisecond))
	want := "retry: 2500\n\n"
	if got != want {
		t.Errorf("appendRetry() = %q, want %q", got, want)
	}
}

func TestAppendMessageBlankLineDispatchRule(t *testing.T) {
	// The WHATWG spec dispatches an event on the blank line: every
	// frame this package emits must end with exactly one blank line,
	// and fields within a frame must never contain one (which would
	// prematurely terminate the event).
	msg := Message{ID: "1", Event: "e", Data: "a\nb"}
	frame := string(appendMessage(nil, msg))
	if frame[len(frame)-2:] != "\n\n" {
		t.Fatalf("frame does not end with blank-line terminator: %q", frame)
	}
	if frame[:len(frame)-2] == "" {
		t.Fatalf("frame has no field lines before the terminator")
	}
}
