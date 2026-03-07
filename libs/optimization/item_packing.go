package optimization

import (
	"cmp"
	"encoding/json"
	"slices"

	"github.com/joaqu1m/sqs-batching-manager/libs/internal_errors"
)

const COMPRESS_QUEUE_VALUES = true

type JSONItem struct {
	Content         map[string]any
	MarshalledAlone []byte
}

type ErroredJSONItem struct {
	Content map[string]any
	Err     error
}

func PackIntoSQSBatches(
	itemsToBePacked []map[string]any,
	targetSizeThresholds []uint64,
) (packedItems [][]byte, erroredItems []ErroredJSONItem) {

	erroredItems = []ErroredJSONItem{}

	// Ensure thresholds are sorted ascending
	slices.Sort(targetSizeThresholds)

	// The last threshold acts as the absolute maximum
	maxThreshold := targetSizeThresholds[len(targetSizeThresholds)-1]

	// Initially calculate each marshalled item size to do both things at once:
	byteItemsToBePacked := []JSONItem{}
	for _, content := range itemsToBePacked {

		// 1. Marshal the item alone into an array to see if it already exceeds the max threshold by itself.
		bytes, err := MarshalMessageBody([]map[string]any{content})
		if err != nil {
			erroredItems = append(erroredItems, ErroredJSONItem{
				Content: content,
				Err:     internal_errors.WrapSerializationError(err),
			})
			continue
		}
		if uint64(len(bytes)) > maxThreshold {
			erroredItems = append(erroredItems, ErroredJSONItem{
				Content: content,
				Err:     internal_errors.NewExceededAllowedSizeError(int64(len(bytes))),
			})
			continue
		}

		// 2. We include it in the packing process with its size to be ordered and used by First-Fit-Decreasing algo.
		byteItemsToBePacked = append(byteItemsToBePacked, JSONItem{
			Content:         content,
			MarshalledAlone: bytes,
		})
	}

	// Sort items in descending order by SizeWhilePackedAlone (so we can apply 'First Fit Decreasing' algorithm)
	slices.SortFunc(byteItemsToBePacked, func(a, b JSONItem) int {
		return cmp.Compare(len(b.MarshalledAlone), len(a.MarshalledAlone))
	})

	// Let's start the real packing loop
	remaining := make([]JSONItem, len(byteItemsToBePacked))
	copy(remaining, byteItemsToBePacked)

	var packed [][]byte

	for len(remaining) > 0 {

		leader := remaining[0]
		remaining = remaining[1:]

		currentBatch := []map[string]any{leader.Content}
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
		var leftovers []JSONItem
		for _, candidate := range remaining {

			candidateCurrentBatch := append(currentBatch, candidate.Content)
			candidateBatchedBytes, err := MarshalMessageBody(candidateCurrentBatch)
			if err != nil {
				erroredItems = append(erroredItems, ErroredJSONItem{
					Content: candidate.Content,
					Err:     internal_errors.WrapSerializationError(err),
				})
				continue
			}

			if uint64(len(candidateBatchedBytes)) <= batchCeiling {
				currentBatch = candidateCurrentBatch
				currentBatchBytes = candidateBatchedBytes
			} else {
				leftovers = append(leftovers, candidate)
			}
		}

		remaining = leftovers
		packed = append(packed, currentBatchBytes)
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
