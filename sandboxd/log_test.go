package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestJournalWriterPrefixesEveryRecordWithItsSyslogLevel(t *testing.T) {
	tests := []struct {
		name string
		emit func(zerolog.Logger)
		want string
	}{
		{"error", func(l zerolog.Logger) { l.Error().Msg("boom") }, syslogErr},
		{"warn", func(l zerolog.Logger) { l.Warn().Msg("boom") }, syslogWarn},
		{"info", func(l zerolog.Logger) { l.Info().Msg("boom") }, syslogInfo},
		{"debug", func(l zerolog.Logger) { l.Debug().Msg("boom") }, syslogDebug},
		{"no level", func(l zerolog.Logger) { l.Log().Msg("boom") }, syslogInfo},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			tt.emit(testLogger(buf))
			line := buf.String()
			if !strings.HasPrefix(line, tt.want) {
				t.Errorf("line = %q, want prefix %q", line, tt.want)
			}
			if !strings.Contains(line, "boom") {
				t.Errorf("line = %q, want the message", line)
			}
			if strings.Count(line, "\n") != 1 {
				t.Errorf("line = %q, want exactly one record", line)
			}
			if strings.Contains(line, "\x1b[") {
				t.Errorf("line = %q, want no ANSI color in a journald record", line)
			}
		})
	}
}

func TestJournalWriterEmitsOneWritePerRecord(t *testing.T) {
	counter := &countingWriter{}
	logger := testLogger(counter)
	logger.Error().Msg("first")
	logger.Info().Str("k", "v").Msg("second")
	if counter.writes != 2 {
		t.Errorf("writes = %d, want 2", counter.writes)
	}
	if got := strings.Count(counter.buf.String(), syslogErr); got != 1 {
		t.Errorf("error prefixes = %d, want 1", got)
	}
}

func TestSyslogPrefixMapsFatalToCritical(t *testing.T) {
	if got := syslogPrefix(zerolog.FatalLevel); got != syslogCrit {
		t.Errorf("fatal prefix = %q, want %q", got, syslogCrit)
	}
}

func TestJournalWriterKeepsJSONRecordsBehindThePrefix(t *testing.T) {
	counter := &countingWriter{}
	logger := zerolog.New(newJournalWriter(counter, true))
	logger.Error().Str("k", "v").Msg("boom")
	line := counter.buf.String()
	if counter.writes != 1 || !strings.HasPrefix(line, syslogErr+"{") || !json.Valid([]byte(line[len(syslogErr):])) {
		t.Errorf("writes = %d, line = %q, want one write of %q and one JSON record", counter.writes, line, syslogErr)
	}
}

func testLogger(out io.Writer) zerolog.Logger {
	return zerolog.New(newJournalWriter(out, false))
}

type countingWriter struct {
	buf    bytes.Buffer
	writes int
}

func (c *countingWriter) Write(p []byte) (int, error) {
	c.writes++
	return c.buf.Write(p)
}
