package logger

import (
	"io"
	"sync"
)

type LogLevel int

const (
	LevelInfo LogLevel = iota + 1
	LevelWarn
	LevelError
	LevelFatal
)

// Interface logger interface
type Logger interface {
	SetLevel(LogLevel)
	Info(string, ...interface{})
	Warn(string, ...interface{})
	Error(string, ...interface{})
	Fatal(string, ...interface{})
}

type logger struct {
	sync.Mutex
	lvl        LogLevel
	infoLabel  string
	warnLabel  string
	errorLabel string
	fatalLabel string
	w          io.Writer
}

// NewStdLogger output log to command line
func NewStdLogger() *logger {
	return &logger{
		infoLabel:  "[INFO]",
		warnLabel:  "[WARN]",
		errorLabel: "[ERROR]",
		fatalLabel: "[FATAL]",
	}
}

// NewStdLogger output log to a file
func NewFileLogger() *logger {
	return &logger{
		infoLabel:  "[INFO]",
		warnLabel:  "[WARN]",
		errorLabel: "[ERROR]",
		fatalLabel: "[FATAL]",
	}
}

func (l *logger) SetLevel(lvl LogLevel) {
	l.lvl = lvl
}

func (l *logger) Infof(format string, v ...interface{}) {}

func (l *logger) Warnf(format string, v ...interface{}) {}

func (l *logger) Errorf(format string, v ...interface{}) {}

func (l *logger) Fatalf(format string, v ...interface{}) {}
