package logging

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
)

type LogConf []string

func (lc *LogConf) String() string {
	b := bytes.Buffer{}
	for _, s := range *lc {
		b.WriteString(s)
	}
	return b.String()
}

func (lc *LogConf) Set(v string) error {
	*lc = append(*lc, v)
	return nil
}

const (
	LogLevelError = iota
	LogLevelWarning
	LogLevelInfo
	LogLevelDebug
	LogLevelVerbose
	LogLevelTrace
)

var LogLevels = make(map[string]int)
var DefaultLogLevel = LogLevelWarning

type Logger struct {
	Error   *log.Logger
	Warning *log.Logger
	Info    *log.Logger
	Debug   *log.Logger
	Verbose *log.Logger
	Trace   *log.Logger
}

func NewLogger(prefix string) *Logger {
	var traceWriter io.Writer
	var verboseWriter io.Writer
	var debugWriter io.Writer
	var infoWriter io.Writer
	var warnWriter io.Writer
	level := DefaultLogLevel // the default loggin level
	if l, found := LogLevels[prefix]; found {
		level = l
	}
	switch {
	case level == LogLevelError:
		warnWriter = &Sink{}
		infoWriter = &Sink{}
		debugWriter = &Sink{}
		verboseWriter = &Sink{}
		traceWriter = &Sink{}
	case level == LogLevelWarning:
		warnWriter = os.Stdout
		infoWriter = &Sink{}
		debugWriter = &Sink{}
		verboseWriter = &Sink{}
		traceWriter = &Sink{}
	case level == LogLevelInfo:
		warnWriter = os.Stdout
		infoWriter = os.Stdout
		debugWriter = &Sink{}
		verboseWriter = &Sink{}
		traceWriter = &Sink{}
	case level == LogLevelDebug:
		warnWriter = os.Stdout
		infoWriter = os.Stdout
		debugWriter = os.Stdout
		verboseWriter = &Sink{}
		traceWriter = &Sink{}
	case level == LogLevelVerbose:
		warnWriter = os.Stdout
		infoWriter = os.Stdout
		debugWriter = os.Stdout
		verboseWriter = os.Stdout
		traceWriter = &Sink{}
	case level == LogLevelTrace:
		warnWriter = os.Stdout
		infoWriter = os.Stdout
		debugWriter = os.Stdout
		verboseWriter = os.Stdout
		traceWriter = os.Stdout
	}
	return &Logger{
		Error:   log.New(os.Stderr, fmt.Sprintf("ERROR|%s|", prefix), log.Ltime|log.Lmicroseconds),
		Warning: log.New(warnWriter, fmt.Sprintf("WARN|%s|", prefix), log.Ltime|log.Lmicroseconds),
		Info:    log.New(infoWriter, fmt.Sprintf("INFO|%s|", prefix), log.Ltime|log.Lmicroseconds),
		Debug:   log.New(debugWriter, fmt.Sprintf("DEBUG|%s|", prefix), log.Ltime|log.Lmicroseconds),
		Verbose: log.New(verboseWriter, fmt.Sprintf("VERBOSE|%s|", prefix), log.Ltime|log.Lmicroseconds),
		Trace:   log.New(traceWriter, fmt.Sprintf("TRACE|%s|", prefix), log.Ltime|log.Lmicroseconds),
	}
}

type Sink struct{}

func (s *Sink) Write(p []byte) (int, error) {
	return len(p), nil
}
