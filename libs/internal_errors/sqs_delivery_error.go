package internal_errors

import (
	"fmt"
	"runtime"
)

// SQSDeliveryError represents Marshal or Unmarshal errors
type SQSDeliveryError struct {
	File string
	Line int

	// Indicates whether the message ID was successfully parsed from the SQS response. This can be useful for debugging and error handling, as it provides insight into whether the failure was due to an issue with parsing the response or if it was a different kind of error.
	WasIDParsedSuccesfully bool

	// An error code representing why the action failed on this entry.
	Code string

	// Specifies whether the error happened due to the caller of the batch API action.
	SenderFault bool

	// A message explaining why the action failed on this entry.
	Message *string
}

// SQSDeliveryError represents errors encountered when delivering messages to SQS using "SendMessage" or "SendMessageBatch" API actions.
func NewSQSDeliveryError(wasIDParsedSuccesfully bool, code string, senderFault bool, message *string) *SQSDeliveryError {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	}

	return &SQSDeliveryError{
		File:                   file,
		Line:                   line,
		WasIDParsedSuccesfully: wasIDParsedSuccesfully,
		Code:                   code,
		SenderFault:            senderFault,
		Message:                message,
	}
}

func (e SQSDeliveryError) Error() string {

	criticalErrorPrefix := ""
	if !e.WasIDParsedSuccesfully {
		criticalErrorPrefix = "Critical "
	}

	message := "No message provided"
	if e.Message != nil {
		message = *e.Message
	}

	return fmt.Sprintf("%sSQSDelivery error at (%s:%d): [Code %s] [Sender Fault: %t] %s", criticalErrorPrefix, e.File, e.Line, e.Code, e.SenderFault, message)
}
