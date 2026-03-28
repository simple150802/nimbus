package logging

import "fmt"

const (
	reset  = "\033[0m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	blue   = "\033[34m"
	white  = "\033[97m"
)

func Stage(args ...any) {
	// fmt.Sprintln automatically adds spaces between arguments and a newline at the end.
	msg := fmt.Sprintln(args...)
	// We strip the trailing newline using msg[:len(msg)-1] so the color reset
	// happens on the same line, preventing color bleed in the terminal.
	Info("\n------------------------------------------------")
	fmt.Printf("%s%s%s\n", blue, msg[:len(msg)-1], reset)
}

// Info prints space-separated arguments in blue
func Info(args ...any) {
	// fmt.Sprintln automatically adds spaces between arguments and a newline at the end.
	msg := fmt.Sprintln(args...)
	// We strip the trailing newline using msg[:len(msg)-1] so the color reset
	// happens on the same line, preventing color bleed in the terminal.
	fmt.Printf("%s%s%s\n", blue, msg[:len(msg)-1], reset)
}

// Success prints space-separated arguments in green
func Success(args ...any) {
	msg := fmt.Sprintln(args...)
	fmt.Printf("%s%s%s\n", green, msg[:len(msg)-1], reset)
}

// Failure prints space-separated arguments in red
func Failure(args ...any) {
	msg := fmt.Sprintln(args...)
	fmt.Printf("%s%s%s\n", red, msg[:len(msg)-1], reset)
}

// Warning prints space-separated arguments in yellow
func Warning(args ...any) {
	msg := fmt.Sprintln(args...)
	fmt.Printf("%s%s%s\n", yellow, msg[:len(msg)-1], reset)
}

// Normal prints space-separated arguments in standard white
func Normal(args ...any) {
	msg := fmt.Sprintln(args...)
	fmt.Printf("%s%s%s\n", white, msg[:len(msg)-1], reset)
}
