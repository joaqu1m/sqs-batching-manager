package internal_errors

import (
	"fmt"
	"runtime"
)

// ExceededAllowedSizeError represents errors when the maximum allowed size is reached
type ExceededAllowedSizeError struct {
	File     string
	Line     int
	ItemSize uint64
}

// ExceededAllowedSizeError represents errors when the maximum allowed size is reached
func NewExceededAllowedSizeError(itemSize uint64) ExceededAllowedSizeError {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	}

	return ExceededAllowedSizeError{
		File:     file,
		Line:     line,
		ItemSize: itemSize,
	}
}

func (e ExceededAllowedSizeError) Error() string {
	return fmt.Sprintf("Maximum allowed size reached at (%s:%d): %d", e.File, e.Line, e.ItemSize)
}
