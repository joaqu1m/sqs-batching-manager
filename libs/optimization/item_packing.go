package optimization

import (
	"cmp"
	"encoding/json"
	"slices"

	"github.com/joaqu1m/sqs-batching-manager/libs/internal_errors"
)

const COMPRESS_QUEUE_VALUES = true

// SQSItem is one logical message offered to PackIntoSQSBatches. MessageID is opaque to this package — it is
// the caller-provided identifier that flows through packing and resurfaces in PackedBatch.MessageIDs /
// ErroredSQSItem.MessageID.
type SQSItem struct {
	MessageID string
	Content   map[string]any
}

// ErroredSQSItem is a logical message that could not be packed (serialization failure, or it exceeds the
// largest threshold on its own). Only the MessageID is reported back — the caller still holds the original content.
type ErroredSQSItem struct {
	MessageID string
	Err       error
}

// PackedBatch is one packed SQS message body: the serialized bytes that will become MessageBody, alongside
// the MessageIDs of the logical messages packed into it. MessageIDs are kept so failure reporting can
// identify exactly which caller-side items the batch represented, without re-unmarshaling the bytes.
type PackedBatch struct {
	Bytes      []byte
	MessageIDs []string
}

// packingItem is the internal working tuple used during the First-Fit-Decreasing loop: it carries the same
// data as SQSItem plus the pre-computed standalone marshal used for ordering and size checks.
type packingItem struct {
	MessageID       string
	Content         map[string]any
	MarshalledAlone []byte
}

func PackIntoSQSBatches(
	itemsToBePacked []SQSItem,
	targetSizeThresholds []uint64,
) (packedItems []PackedBatch, erroredItems []ErroredSQSItem) {

	erroredItems = []ErroredSQSItem{}

	// Ensure thresholds are sorted ascending
	slices.Sort(targetSizeThresholds)

	// The last threshold acts as the absolute maximum
	maxThreshold := targetSizeThresholds[len(targetSizeThresholds)-1]

	// Initially calculate each marshalled item size to do both things at once:
	byteItemsToBePacked := []packingItem{}
	for _, item := range itemsToBePacked {

		// 1. Marshal the item alone into an array to see if it already exceeds the max threshold by itself.
		bytes, err := MarshalMessageBody([]map[string]any{item.Content})
		if err != nil {
			erroredItems = append(erroredItems, ErroredSQSItem{
				MessageID: item.MessageID,
				Err:       internal_errors.WrapSerializationError(err),
			})
			continue
		}
		if uint64(len(bytes)) > maxThreshold {
			erroredItems = append(erroredItems, ErroredSQSItem{
				MessageID: item.MessageID,
				Err:       internal_errors.NewExceededAllowedSizeError(uint64(len(bytes))),
			})
			continue
		}

		// 2. We include it in the packing process with its size to be ordered and used by First-Fit-Decreasing algo.
		byteItemsToBePacked = append(byteItemsToBePacked, packingItem{
			MessageID:       item.MessageID,
			Content:         item.Content,
			MarshalledAlone: bytes,
		})
	}

	// Sort items in descending order by SizeWhilePackedAlone (so we can apply 'First Fit Decreasing' algorithm)
	slices.SortFunc(byteItemsToBePacked, func(a, b packingItem) int {
		return cmp.Compare(len(b.MarshalledAlone), len(a.MarshalledAlone))
	})

	// Let's start the real packing loop
	remaining := make([]packingItem, len(byteItemsToBePacked))
	copy(remaining, byteItemsToBePacked)

	var packed []PackedBatch

	for len(remaining) > 0 {

		leader := remaining[0]
		remaining = remaining[1:]

		currentBatch := []map[string]any{leader.Content}
		currentBatchMessageIDs := []string{leader.MessageID}
		currentBatchBytes := leader.MarshalledAlone

		// Find the smallest threshold that can accommodate the current batch
		batchCeiling := maxThreshold
		for _, threshold := range targetSizeThresholds {
			if uint64(len(currentBatchBytes)) <= threshold {
				batchCeiling = threshold
				break
			}
		}

		// Try to fill the remaining space with smaller items
		var leftovers []packingItem
		for _, candidate := range remaining {

			candidateCurrentBatch := append(currentBatch, candidate.Content)
			candidateBatchedBytes, err := MarshalMessageBody(candidateCurrentBatch)
			if err != nil {
				erroredItems = append(erroredItems, ErroredSQSItem{
					MessageID: candidate.MessageID,
					Err:       internal_errors.WrapSerializationError(err),
				})
				continue
			}

			if uint64(len(candidateBatchedBytes)) <= batchCeiling {
				currentBatch = candidateCurrentBatch
				currentBatchMessageIDs = append(currentBatchMessageIDs, candidate.MessageID)
				currentBatchBytes = candidateBatchedBytes
			} else {
				leftovers = append(leftovers, candidate)
			}
		}

		remaining = leftovers
		packed = append(packed, PackedBatch{
			Bytes:      currentBatchBytes,
			MessageIDs: currentBatchMessageIDs,
		})
	}

	return packed, erroredItems
}

func MarshalMessageBody(v any) ([]byte, error) {

	if COMPRESS_QUEUE_VALUES {
		return MarshalOptimized(v)
	}

	return json.Marshal(v)

}

func UnmarshalMessageBody(data []byte, v any) error {

	if COMPRESS_QUEUE_VALUES {
		return UnmarshalOptimized(data, v)
	}

	return json.Unmarshal(data, v)

}
