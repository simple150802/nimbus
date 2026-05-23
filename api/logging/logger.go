package logging

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	reset  = "\033[0m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	blue   = "\033[34m"
	white  = "\033[97m"
)

// File-mirror plumbing. logFile is nil when file logging is disabled
// (env override = empty / /dev/null, or open failed). fileMu serializes
// writes from concurrent goroutines (watcher, worker, ksvc watcher,
// probes) so multi-line messages don't interleave in the on-disk log.
var (
	fileMu  sync.Mutex
	logFile *os.File
)

func init() {
	path := os.Getenv("NIMBUS_LOG_FILE")
	if path == "" {
		path = "./nimbus.log"
	}
	if path == "/dev/null" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[logging] failed to open %s: %v — file logging disabled\n", path, err)
		return
	}
	logFile = f
	fmt.Fprintf(f, "\n=== nimbus started at %s ===\n", time.Now().UTC().Format(time.RFC3339))
	// Surface the destination to stdout on startup so the user knows
	// where to tail. Bypasses emit() to avoid recursion before init
	// fully returns.
	fmt.Printf("%s[logging] writing to %s%s\n", blue, path, reset)
}

// emit is the single sink for every public logging function. It writes
// the colored line to stdout (preserving the current TTY look) and a
// plain, timestamped, level-tagged line to the file mirror when one is
// configured. Trailing newline stripping mirrors the original
// implementation so the color reset stays on the same line.
func emit(color, level string, args ...any) {
	msg := fmt.Sprintln(args...)
	body := msg[:len(msg)-1]
	fmt.Printf("%s%s%s\n", color, body, reset)
	if logFile == nil {
		return
	}
	fileMu.Lock()
	defer fileMu.Unlock()
	fmt.Fprintf(logFile, "%s [%s] %s\n", time.Now().UTC().Format(time.RFC3339Nano), level, body)
}

func Stage(args ...any) {
	emit(blue, "STAGE", "\n------------------------------------------------")
	emit(blue, "STAGE", args...)
}

// Info prints space-separated arguments in blue
func Info(args ...any) {
	emit(blue, "INFO", args...)
}

// Success prints space-separated arguments in green
func Success(args ...any) {
	emit(green, "OK", args...)
}

// Failure prints space-separated arguments in red
func Failure(args ...any) {
	emit(red, "FAIL", args...)
}

// Warning prints space-separated arguments in yellow
func Warning(args ...any) {
	emit(yellow, "WARN", args...)
}

// Normal prints space-separated arguments in standard white
func Normal(args ...any) {
	emit(white, "LOG", args...)
}
