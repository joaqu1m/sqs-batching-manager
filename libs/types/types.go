package types

type QueueReference struct {
	Name string
	URL  string
}

// AddMessageInput is a single logical message offered to the producer. MessageID is caller-provided and
// surfaces back in FlushErroredMessage.IDs on failure — the producer does not enforce uniqueness; callers
// should pick IDs they can map back to their own state. An empty MessageID is treated as a validation error
// and reported as such on the next Flush().
type AddMessageInput struct {
	QueueName         string
	MessageID         string
	MessageBody       map[string]any
	MessageAttributes map[string]any
	MessageGroupID    *string
}

// FlushOutput is the result of a Flush() call. Errors aggregates every failure observed in this flush plus any
// failures carried over from previous auto-flushes. An empty Errors slice means every message Add()-ed since the
// last successful Flush() reached SQS.
type FlushOutput struct {
	Errors []FlushErroredMessage
}

// FlushErroredMessage reports a failure for one logical message identified by its caller-provided MessageID.
// When a packed SQS entry containing N logical messages fails as a unit, the producer fans the failure out
// into N FlushErroredMessage records — one per MessageID, all sharing the same Err. Only the MessageID is
// returned; the producer does not retain the original message contents, so the caller must keep their own
// copy if they want to retry.
type FlushErroredMessage struct {
	QueueName string
	MessageID string
	Err       error
}
