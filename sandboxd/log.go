package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"time"

	"github.com/projecteru2/core/log"
	coretypes "github.com/projecteru2/core/types"
	"github.com/rs/zerolog"
)

const (
	syslogCrit  = "<2>"
	syslogErr   = "<3>"
	syslogWarn  = "<4>"
	syslogInfo  = "<6>"
	syslogDebug = "<7>"
)

var _ zerolog.LevelWriter = journalWriter{}

// journalWriter prefixes each record with its syslog level: journald classifies a line by the leading <N> and strips it.
type journalWriter struct {
	console zerolog.ConsoleWriter
	out     io.Writer
	json    bool
}

func (j journalWriter) Write(p []byte) (int, error) { return j.WriteLevel(zerolog.NoLevel, p) }

func (j journalWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	buf := &bytes.Buffer{}
	buf.WriteString(syslogPrefix(level))
	if j.json {
		buf.Write(p)
	} else {
		j.console.Out = buf
		if _, err := j.console.Write(p); err != nil {
			return 0, err
		}
	}
	if _, err := j.out.Write(buf.Bytes()); err != nil {
		return 0, err
	}
	return len(p), nil
}

func setupLog(ctx context.Context, level string) error {
	if err := log.SetupLog(ctx, &coretypes.ServerLogConfig{Level: level}, ""); err != nil {
		return err
	}
	logger := log.GetGlobalLogger()
	*logger = zerolog.New(newJournalWriter(os.Stderr, !stderrIsTerminal())).With().Timestamp().Logger().Level(logger.GetLevel())
	return nil
}

func newJournalWriter(out io.Writer, json bool) journalWriter {
	return journalWriter{console: zerolog.ConsoleWriter{TimeFormat: time.RFC822, NoColor: true}, out: out, json: json}
}

func syslogPrefix(level zerolog.Level) string {
	switch level {
	case zerolog.PanicLevel, zerolog.FatalLevel:
		return syslogCrit
	case zerolog.ErrorLevel:
		return syslogErr
	case zerolog.WarnLevel:
		return syslogWarn
	case zerolog.DebugLevel, zerolog.TraceLevel:
		return syslogDebug
	default:
		return syslogInfo
	}
}

func stderrIsTerminal() bool {
	fi, err := os.Stderr.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
