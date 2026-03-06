package optimization

import (
	"cmp"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/joaqu1m/sqs-batching-manager/libs/entities"
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

	attrs := entities.MessageAttributes(msg.MessageAttributes)
	attrsSize := uint64(attrs.GetAWSSizeInBytes())

	return bodySize + attrsSize, nil
}

type sqsItem struct {
	entry types.SendMessageBatchRequestEntry
	size  uint64
}

func PackMessagesIntoRequests(
	messages []types.SendMessageBatchRequestEntry,
	AWSSendMessageBatchMaxTotalPayloadSizeBytes uint64,
	awsSendMessageBatchMaxMessagesCount uint64,
) ([][]types.SendMessageBatchRequestEntry, []SendMessageBatchRequestEntryError) {

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
		if size > AWSSendMessageBatchMaxTotalPayloadSizeBytes {
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
			fits := currentSize+candidate.size <= AWSSendMessageBatchMaxTotalPayloadSizeBytes &&
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
