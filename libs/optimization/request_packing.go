package optimization

import (
	"cmp"
	"encoding/json"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/joaqu1m/sqs-batching-manager/libs/internal_errors"
)

type SendMessageBatchRequestEntryError struct {
	Message types.SendMessageBatchRequestEntry
	Err     error
}

// sizeOfEntry returns the byte size of a single SendMessageBatchRequestEntry,
// calculated as len(MessageBody) + len(json.Marshal(MessageAttributes)).
func sizeOfEntry(msg types.SendMessageBatchRequestEntry) (uint64, error) {
	bodySize := uint64(0)
	if msg.MessageBody != nil {
		bodySize = uint64(len(*msg.MessageBody))
	}

	attrsSize := uint64(0)
	if len(msg.MessageAttributes) > 0 {
		attrsBytes, err := json.Marshal(msg.MessageAttributes)
		if err != nil {
			return 0, err
		}
		attrsSize = uint64(len(attrsBytes))
	}

	return bodySize + attrsSize, nil
}

type sqsItem struct {
	entry types.SendMessageBatchRequestEntry
	size  uint64
}

func PackMessagesIntoRequests(
	messages []types.SendMessageBatchRequestEntry,
	awsSendMessageBatchMaxTotalPayloadSizeKiB uint64,
	awsSendMessageBatchMaxMessagesCount uint64,
) ([][]types.SendMessageBatchRequestEntry, []SendMessageBatchRequestEntryError) {

	maxBytes := awsSendMessageBatchMaxTotalPayloadSizeKiB * 1024

	erroredItems := []SendMessageBatchRequestEntryError{}

	// Pre-calculate sizes and filter out items that already exceed the limit alone.
	items := []sqsItem{}
	for _, msg := range messages {
		size, err := sizeOfEntry(msg)
		if err != nil {
			erroredItems = append(erroredItems, SendMessageBatchRequestEntryError{
				Message: msg,
				Err:     internal_errors.WrapSerializationError(err),
			})
			continue
		}
		if size > maxBytes {
			erroredItems = append(erroredItems, SendMessageBatchRequestEntryError{
				Message: msg,
				Err:     internal_errors.NewExceededAllowedSizeError(int64(size)),
			})
			continue
		}
		items = append(items, sqsItem{entry: msg, size: size})
	}

	// Sort descending by size (First-Fit Decreasing).
	slices.SortFunc(items, func(a, b sqsItem) int {
		return cmp.Compare(b.size, a.size)
	})

	var batches [][]types.SendMessageBatchRequestEntry

	remaining := make([]sqsItem, len(items))
	copy(remaining, items)

	for len(remaining) > 0 {
		leader := remaining[0]
		remaining = remaining[1:]

		currentBatch := []types.SendMessageBatchRequestEntry{leader.entry}
		currentSize := leader.size

		var leftovers []sqsItem
		for _, candidate := range remaining {
			fits := currentSize+candidate.size <= maxBytes &&
				uint64(len(currentBatch)) < awsSendMessageBatchMaxMessagesCount
			if fits {
				currentBatch = append(currentBatch, candidate.entry)
				currentSize += candidate.size
			} else {
				leftovers = append(leftovers, candidate)
			}
		}

		remaining = leftovers
		batches = append(batches, currentBatch)
	}

	return batches, erroredItems
}
