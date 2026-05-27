package producer

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/joaqu1m/sqs-batching-manager/libs/constants"
	"github.com/joaqu1m/sqs-batching-manager/libs/entities"
	"github.com/joaqu1m/sqs-batching-manager/libs/internal_errors"
	"github.com/joaqu1m/sqs-batching-manager/libs/optimization"
	qbm_types "github.com/joaqu1m/sqs-batching-manager/libs/types"
)

// Here we'll group items by 2 factors, to help the Flush method to decide how to batch them:

// 1. By queue, since we can't batch items from different queues together anyways
// 2. By a composite key of (MessageGroupID, marshaledMessageAttributes), since messages with different
//    group IDs or different message attributes can't share a batch entry.

// groupKey uniquely identifies a batch group within a queue.
type groupKey struct {
	MessageGroupID string // empty string means no group
	MarshaledAttrs string // JSON-marshaled message attributes; empty string means no attributes
}

type groupValue struct {
	MessageGroupID    *string        // nullable
	MessageAttributes map[string]any // nullable
	Messages          []optimization.SQSItem
}

type Queue struct {
	QueueReference qbm_types.QueueReference
	Groups         map[groupKey]groupValue
}

type QBMProducer struct {
	ctx             context.Context
	sqsClient       *sqs.Client
	mu              sync.Mutex
	referenceQueues map[string]qbm_types.QueueReference
	queues          map[string]Queue
	// This is not an exact size of the total messages in memory, but it's a good approximation that allows us to decide when to auto flush.
	// It isn't also the exact size of messages according to AWS SQS, since we are really counting it for internal memory management.
	queuesTotalSize uint64
	// failedDelayedMessages holds every failure that happened during an auto-flush, each carrying only the
	// caller-provided MessageID of the affected message (not the body). They sit here, in the producer, until
	// the caller invokes Flush() manually — at which point they are appended to the returned
	// FlushOutput.Errors and cleared. This is what lets Add() stay fire-and-forget on the success path.
	// Trade-off: this slice grows unbounded between manual flushes. The caller MUST call Flush() before
	// discarding the producer, otherwise both the buffered messages and the failure records leave with it,
	// and the caller will not know which MessageIDs failed.
	failedDelayedMessages []qbm_types.FlushErroredMessage
}

// entryOrigin records, per SendMessageBatchRequestEntry built during Flush(), the MessageIDs of the logical
// messages packed into it and the queue it targets. Keyed by entry.Id (the sequential integer we stringify as
// the AWS batch ID), it is the sidecar error paths consult to surface caller MessageIDs instead of payload
// contents.
type entryOrigin struct {
	MessageIDs []string
	QueueName  string
}

// expandEntryErrors fans an entry-level failure out into one FlushErroredMessage per MessageID packed into
// that entry — all sharing the same Err. When the origin lookup fails (entry.Id missing/unparseable or no
// recorded origin) we still emit a single stub record with an empty MessageID so the "nothing silently
// dropped" invariant survives, even though at that point we cannot tell the caller which item(s) were
// affected.
func expandEntryErrors(
	origins map[int]entryOrigin,
	fallbackQueueName string,
	entry types.SendMessageBatchRequestEntry,
	err error,
) []qbm_types.FlushErroredMessage {
	if entry.Id != nil {
		if idx, parseErr := strconv.Atoi(*entry.Id); parseErr == nil {
			if origin, ok := origins[idx]; ok {
				out := make([]qbm_types.FlushErroredMessage, 0, len(origin.MessageIDs))
				for _, messageID := range origin.MessageIDs {
					out = append(out, qbm_types.FlushErroredMessage{
						QueueName: origin.QueueName,
						MessageID: messageID,
						Err:       err,
					})
				}
				return out
			}
		}
	}
	return []qbm_types.FlushErroredMessage{{
		QueueName: fallbackQueueName,
		Err:       err,
	}}
}

// NewQueueBatchingManagerProducer builds a producer bound to the given SQS client and a static map of queues
// (name → URL) that Add() is allowed to target. Adding a message for a queue name absent from this map yields a
// resource-not-found error that surfaces on the next Flush().
func NewQueueBatchingManagerProducer(ctx context.Context, sqsClient *sqs.Client, referenceQueues map[string]qbm_types.QueueReference) *QBMProducer {
	return &QBMProducer{
		ctx:                   ctx,
		sqsClient:             sqsClient,
		referenceQueues:       referenceQueues,
		queues:                map[string]Queue{},
		queuesTotalSize:       0,
		failedDelayedMessages: []qbm_types.FlushErroredMessage{},
	}
}

// Add buffers a message for later delivery. It is fire-and-forget: validation, packing and AWS errors are NOT
// returned here — they are recorded inside the producer and surfaced on the next Flush() as FlushErroredMessages
// carrying the caller-provided ID (not the body).
//
// addMessageInput.MessageID must be non-empty; an empty ID is treated as a validation error and recorded for the next
// Flush(). The producer does not enforce ID uniqueness across calls — callers should choose IDs they can map
// back to their own bookkeeping.
//
// May synchronously trigger an auto-flush when the internal buffer exceeds MAX_SIZE_BEFORE_FLUSHING.
func (qbm *QBMProducer) Add(addMessageInput qbm_types.AddMessageInput) {

	qbm.autoFlushIfNeeded()

	qbm.mu.Lock()
	defer qbm.mu.Unlock()

	// === Checking if the Added Message is valid
	if addMessageInput.MessageID == "" {
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, qbm_types.FlushErroredMessage{
			QueueName: addMessageInput.QueueName,
			MessageID: "",
			Err:       errors.New("AddMessageInput.MessageID is required"),
		})
		return
	}
	if len(addMessageInput.MessageBody) == 0 {
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, qbm_types.FlushErroredMessage{
			QueueName: addMessageInput.QueueName,
			MessageID: addMessageInput.MessageID,
			Err:       errors.New("AddMessageInput.MessageBody is required"),
		})
		return
	}
	messageBodyBytes, err := json.Marshal(addMessageInput.MessageBody)
	if err != nil {
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, qbm_types.FlushErroredMessage{
			QueueName: addMessageInput.QueueName,
			MessageID: addMessageInput.MessageID,
			Err:       internal_errors.WrapSerializationError(err),
		})
		return
	}
	// here we only check if MessageAttributes exceeds the maximum allowed, since MessageBody has its own complex rules that are checked later
	maxThreshold := constants.AWSSQSChargingThresholds[len(constants.AWSSQSChargingThresholds)-1]
	messageAttributes := entities.NewMessageAttributesFromMap(addMessageInput.MessageAttributes)
	if messageAttributes.GetAWSSizeInBytes() >= maxThreshold {
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, qbm_types.FlushErroredMessage{
			QueueName: addMessageInput.QueueName,
			MessageID: addMessageInput.MessageID,
			Err:       internal_errors.NewExceededAllowedSizeError(messageAttributes.GetAWSSizeInBytes()),
		})
		return
	}
	// ===

	if err := qbm.ensureQueueExistance(addMessageInput.QueueName); err != nil {
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, qbm_types.FlushErroredMessage{
			QueueName: addMessageInput.QueueName,
			MessageID: addMessageInput.MessageID,
			Err:       err,
		})
		return
	}

	queue := qbm.queues[addMessageInput.QueueName]

	// Build the composite group key
	marshaledAttrs := ""
	if len(addMessageInput.MessageAttributes) > 0 {
		if attrsBytes, err := messageAttributes.Marshal(); err == nil {
			marshaledAttrs = string(attrsBytes)
		}
	}
	messageGroupIDStr := ""
	if addMessageInput.MessageGroupID != nil {
		messageGroupIDStr = *addMessageInput.MessageGroupID
	}

	key := groupKey{
		MessageGroupID: messageGroupIDStr,
		MarshaledAttrs: marshaledAttrs,
	}

	group, ok := queue.Groups[key]
	if !ok {
		group = groupValue{
			MessageGroupID: addMessageInput.MessageGroupID,
			Messages:       []optimization.SQSItem{},
		}
		if len(addMessageInput.MessageAttributes) > 0 {
			group.MessageAttributes = addMessageInput.MessageAttributes
		}
	}
	group.Messages = append(group.Messages, optimization.SQSItem{
		MessageID: addMessageInput.MessageID,
		Content:   addMessageInput.MessageBody,
	})
	queue.Groups[key] = group

	qbm.queues[addMessageInput.QueueName] = queue

	qbm.queuesTotalSize += uint64(len(messageBodyBytes)) + messageAttributes.GetAWSSizeInBytes()

}

const MAX_SIZE_BEFORE_FLUSHING = 1024 * 1024 * 50 // 50 MiB

func (qbm *QBMProducer) autoFlushIfNeeded() {

	qbm.mu.Lock()
	needsAutoFlush := qbm.queuesTotalSize >= MAX_SIZE_BEFORE_FLUSHING
	qbm.mu.Unlock()

	if needsAutoFlush {
		flushOutput := qbm.Flush()

		// we can't lock the whole autoFlushIfNeeded function altogether, since the Flush method already locks the mutex;
		// but it's ok, the user logically still needs to flush one more time after the Add() method,
		// so no failed message will be lost to TOCTOU, they will just be delayed until the next flush.
		qbm.mu.Lock()
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, flushOutput.Errors...)
		qbm.mu.Unlock()
	}

}

// Flush drains all buffered messages, dispatches them to SQS, and returns every failure observed at any stage —
// including failures accumulated by prior auto-flushes. After Flush() returns, the producer's internal state is
// empty: the caller is then free to drop the references they used to build the messages.
//
// Callers MUST invoke Flush() before discarding those references (end of request, end of batch job, shutdown);
// otherwise both the buffered messages and any pending failure records are lost with the producer.
func (qbm *QBMProducer) Flush() qbm_types.FlushOutput {
	erroredItems := []qbm_types.FlushErroredMessage{}

	qbm.mu.Lock()
	queues := qbm.queues
	qbm.queues = map[string]Queue{}
	qbm.queuesTotalSize = 0
	erroredItems = append(erroredItems, qbm.failedDelayedMessages...)
	qbm.failedDelayedMessages = []qbm_types.FlushErroredMessage{}
	qbm.mu.Unlock()

	for _, queue := range queues {

		sendMessageBatchRequestEntries := []types.SendMessageBatchRequestEntry{}
		// origins is the source-of-truth sidecar for this queue's flush: maps the entry index (the integer we
		// also stringify into entry.Id) back to the logical messages packed into it. Error paths read from
		// here instead of unmarshaling the AWS payload, so failures preserve every logical message.
		origins := map[int]entryOrigin{}

		for _, group := range queue.Groups {

			messageAttributes := entities.NewMessageAttributesFromMap(group.MessageAttributes)
			messageAttributesSize := messageAttributes.GetAWSSizeInBytes()

			// We need to subtract the size of the message attributes from the total allowed batch size, since all messages in the batch will share the same attributes.
			// That's why we call this variable "Message Body Size Thresholds", because it's the maximum allowed size for the message body, after accounting for the attributes size.
			messageBodySizeThresholds := slices.Clone(constants.AWSSQSChargingThresholds)
			for i, threshold := range messageBodySizeThresholds {
				messageBodySizeThresholds[i] = threshold - messageAttributesSize
			}

			packedBatches, newErroredItems := optimization.PackIntoSQSBatches(group.Messages, messageBodySizeThresholds)
			for _, newErroredItem := range newErroredItems {
				erroredItems = append(erroredItems, qbm_types.FlushErroredMessage{
					QueueName: queue.QueueReference.Name,
					MessageID: newErroredItem.MessageID,
					Err:       newErroredItem.Err,
				})
			}

			for _, packedBatch := range packedBatches {
				entryIdx := len(sendMessageBatchRequestEntries)
				entry := types.SendMessageBatchRequestEntry{
					Id:          aws.String(strconv.Itoa(entryIdx)),
					MessageBody: aws.String(string(packedBatch.Bytes)),
				}
				if !messageAttributes.IsEmpty() {
					entry.MessageAttributes = messageAttributes
				}
				if group.MessageGroupID != nil {
					entry.MessageGroupId = aws.String(*group.MessageGroupID)
				}
				sendMessageBatchRequestEntries = append(sendMessageBatchRequestEntries, entry)
				origins[entryIdx] = entryOrigin{
					MessageIDs: packedBatch.MessageIDs,
					QueueName:  queue.QueueReference.Name,
				}
			}

		}

		entryPacks, packingErrors := optimization.PackMessagesIntoRequests(
			sendMessageBatchRequestEntries,
			constants.AWSSendMessageBatchMaxTotalPayloadSize,
			constants.AWSSendMessageBatchMaxMessagesCount,
		)
		for _, packingError := range packingErrors {
			erroredItems = append(erroredItems, expandEntryErrors(origins, queue.QueueReference.Name, packingError.Message, packingError.Err)...)
		}

		erroredItems = append(erroredItems, qbm.sendBatchesConcurrently(entryPacks, origins, queue)...)

	}

	return qbm_types.FlushOutput{
		Errors: erroredItems,
	}
}

func (qbm *QBMProducer) sendBatchesConcurrently(
	entryPacks [][]types.SendMessageBatchRequestEntry,
	origins map[int]entryOrigin,
	queue Queue,
) []qbm_types.FlushErroredMessage {
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		allErrors []qbm_types.FlushErroredMessage
	)

	for _, entries := range entryPacks {
		wg.Add(1)
		go func(entries []types.SendMessageBatchRequestEntry) {
			defer wg.Done()

			batchErrors := qbm.processSingleBatch(entries, origins, queue)

			if len(batchErrors) > 0 {
				mu.Lock()
				allErrors = append(allErrors, batchErrors...)
				mu.Unlock()
			}
		}(entries)
	}

	wg.Wait()
	return allErrors
}

func (qbm *QBMProducer) processSingleBatch(
	entries []types.SendMessageBatchRequestEntry,
	origins map[int]entryOrigin,
	queue Queue,
) []qbm_types.FlushErroredMessage {
	var batchErrors []qbm_types.FlushErroredMessage

	sendMessageBatchOutput, err := qbm.sqsClient.SendMessageBatch(qbm.ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queue.QueueReference.URL),
		Entries:  entries,
	})
	if err != nil {
		// Whole request failed: every entry — and every logical message packed inside each entry — is errored.
		for _, entry := range entries {
			batchErrors = append(batchErrors, expandEntryErrors(origins, queue.QueueReference.Name, entry, err)...)
		}
		return batchErrors
	}

	for _, failed := range sendMessageBatchOutput.Failed {
		code := "unknown"
		if failed.Code != nil {
			code = *failed.Code
		}

		// Try to identify the source entry from failed.Id. Anything that prevents a lookup (nil Id, bad parse,
		// missing origin) collapses to a stub record with WasItemIdentified=false and an empty MessageID.
		var origin entryOrigin
		identified := false
		if failed.Id != nil {
			if idx, parseErr := strconv.Atoi(*failed.Id); parseErr == nil {
				if o, ok := origins[idx]; ok {
					origin = o
					identified = true
				}
			}
		}
		if !identified {
			batchErrors = append(batchErrors, qbm_types.FlushErroredMessage{
				QueueName: queue.QueueReference.Name,
				Err:       internal_errors.NewSQSDeliveryError(false, code, failed.SenderFault, failed.Message),
			})
			continue
		}

		// Identified: fan out — one record per logical MessageID packed into the failed entry.
		deliveryErr := internal_errors.NewSQSDeliveryError(true, code, failed.SenderFault, failed.Message)
		for _, messageID := range origin.MessageIDs {
			batchErrors = append(batchErrors, qbm_types.FlushErroredMessage{
				QueueName: origin.QueueName,
				MessageID: messageID,
				Err:       deliveryErr,
			})
		}
	}

	return batchErrors
}

func (qbm *QBMProducer) ensureQueueExistance(queueName string) error {

	_, alreadyExists := qbm.queues[queueName]
	if alreadyExists {
		return nil
	}

	queueReference, exists := qbm.referenceQueues[queueName]
	if exists {
		qbm.queues[queueName] = Queue{
			QueueReference: queueReference,
			Groups:         map[groupKey]groupValue{},
		}
		return nil
	}

	return internal_errors.NewResourceNotFoundError(&queueName)

}
