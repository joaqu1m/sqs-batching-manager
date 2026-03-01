package internal_errors

import (
	"fmt"
	"runtime"
)

// SerializationError represents Marshal or Unmarshal errors
type SerializationError struct {
	File string
	Line int
	Err  error
}

// SerializationError represents Marshal or Unmarshal errors
func WrapSerializationError(err error) *SerializationError {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	}

	return &SerializationError{
		File: file,
		Line: line,
		Err:  err,
	}
}

func (e SerializationError) Error() string {
	return fmt.Sprintf("serialization error at (%s:%d): %v", e.File, e.Line, e.Err)
}

func (e SerializationError) Unwrap() error {
	return e.Err
}
