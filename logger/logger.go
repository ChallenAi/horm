package logger

import (
	"fmt"
	"io"
	"os"
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
	Infof(string, ...interface{})
	Warnf(string, ...interface{})
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
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
		lvl:        LevelInfo,
		infoLabel:  "[INFO] ",
		warnLabel:  "[WARN] ",
		errorLabel: "[ERROR] ",
		fatalLabel: "[FATAL] ",
		w:          os.Stdout,
	}
}

// NewStdLogger output log to a file
func NewFileLogger(filePath string) *logger {
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		panic("fail to create log file")
	}
	return &logger{
		lvl:        LevelInfo,
		infoLabel:  "[INFO] ",
		warnLabel:  "[WARN] ",
		errorLabel: "[ERROR] ",
		fatalLabel: "[FATAL] ",
		w:          f,
	}
}

func (l *logger) SetLevel(lvl LogLevel) {
	l.lvl = lvl
}

func (l *logger) Infof(format string, v ...interface{}) {
	fmt.Fprintf(l.w, l.infoLabel+format+"\n", v...)
}

func (l *logger) Warnf(format string, v ...interface{}) {
	fmt.Fprintf(l.w, l.warnLabel+format+"\n", v...)
}

func (l *logger) Errorf(format string, v ...interface{}) {
	fmt.Fprintf(l.w, l.errorLabel+format+"\n", v...)
}

func (l *logger) Fatalf(format string, v ...interface{}) {
	fmt.Fprintf(l.w, l.fatalLabel+format+"\n", v...)
}
