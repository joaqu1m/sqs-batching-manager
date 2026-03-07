package internal_errors

import (
	"fmt"
	"runtime"
)

// ResourceNotFoundError represents errors when a referenced resource is not found
type ResourceNotFoundError struct {
	File              string
	Line              int
	ResourceReference *string
}

// ResourceNotFoundError represents errors when a referenced resource is not found
func NewResourceNotFoundError(resourceReference *string) *ResourceNotFoundError {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	}

	return &ResourceNotFoundError{
		File:              file,
		Line:              line,
		ResourceReference: resourceReference,
	}
}

func (e ResourceNotFoundError) Error() string {
	errorMsgSuffix := ""
	if e.ResourceReference != nil {
		errorMsgSuffix = fmt.Sprintf(": resource %s not found", *e.ResourceReference)
	}
	return fmt.Sprintf("ResourceNotFound error at (%s:%d)%s", e.File, e.Line, errorMsgSuffix)
}
