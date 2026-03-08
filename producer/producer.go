package producer

import (
	"context"
	"encoding/json"
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
	MessageGroupID *string // empty string means no group
	MarshaledAttrs string  // JSON-marshaled message attributes; empty means no attributes
}

type groupValue struct {
	MessageGroupID    *string        // nullable
	MessageAttributes map[string]any // nullable
	Messages          []map[string]any
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
	// Here we save all the messages that failed in auto flushes, so they can be returned in the next manual flush.
	failedDelayedMessages []qbm_types.FlushErroredMessage
}

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

func (qbm *QBMProducer) Add(addMessageInput qbm_types.AddMessageInput) {

	qbm.autoFlushIfNeeded()

	qbm.mu.Lock()
	defer qbm.mu.Unlock()

	// === Testing if the MessageBody is valid
	if len(addMessageInput.MessageBody) == 0 {
		return
	}
	messageBodyBytes, err := json.Marshal(addMessageInput.MessageBody)
	if err != nil {
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, qbm_types.FlushErroredMessage{
			QueueName:         addMessageInput.QueueName,
			MessageBody:       addMessageInput.MessageBody,
			MessageAttributes: addMessageInput.MessageAttributes,
			MessageGroupID:    addMessageInput.MessageGroupID,
			Err:               internal_errors.WrapSerializationError(err),
		})
		return
	}

	// here we only check if MessageAttributes exceeds the maximum allowed, since MessageBody has its own complex rules that are checked later
	maxThreshold := constants.AWSSQSChargingThresholds[len(constants.AWSSQSChargingThresholds)-1]
	messageAttributes := entities.NewMessageAttributesFromMap(addMessageInput.MessageAttributes)
	if messageAttributes.GetAWSSizeInBytes() >= maxThreshold {
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, qbm_types.FlushErroredMessage{
			QueueName:         addMessageInput.QueueName,
			MessageBody:       addMessageInput.MessageBody,
			MessageAttributes: addMessageInput.MessageAttributes,
			MessageGroupID:    addMessageInput.MessageGroupID,
			Err:               internal_errors.NewExceededAllowedSizeError(messageAttributes.GetAWSSizeInBytes()),
		})
		return
	}

	// ===

	if err := qbm.ensureQueueExistance(addMessageInput.QueueName); err != nil {
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, qbm_types.FlushErroredMessage{
			QueueName:         addMessageInput.QueueName,
			MessageBody:       addMessageInput.MessageBody,
			MessageAttributes: addMessageInput.MessageAttributes,
			MessageGroupID:    addMessageInput.MessageGroupID,
			Err:               err,
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

	key := groupKey{
		MessageGroupID: addMessageInput.MessageGroupID,
		MarshaledAttrs: marshaledAttrs,
	}

	group, ok := queue.Groups[key]
	if !ok {
		group = groupValue{
			MessageGroupID: addMessageInput.MessageGroupID,
			Messages:       []map[string]any{},
		}
		if len(addMessageInput.MessageAttributes) > 0 {
			group.MessageAttributes = addMessageInput.MessageAttributes
		}
	}
	group.Messages = append(group.Messages, addMessageInput.MessageBody)
	queue.Groups[key] = group

	qbm.queues[addMessageInput.QueueName] = queue
	qbm.queuesTotalSize += uint64(len(messageBodyBytes)) + uint64(len(marshaledAttrs))

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

		for _, group := range queue.Groups {

			messageAttributes := entities.NewMessageAttributesFromMap(group.MessageAttributes)
			messageAttributesSize := messageAttributes.GetAWSSizeInBytes()

			// We need to subtract the size of the message attributes from the total allowed batch size, since all messages in the batch will share the same attributes.
			// That's why we call this variable "Message Body Size Thresholds", because it's the maximum allowed size for the message body, after accounting for the attributes size.
			messageBodySizeThresholds := slices.Clone(constants.AWSSQSChargingThresholds)
			for i, threshold := range messageBodySizeThresholds {
				messageBodySizeThresholds[i] = threshold - messageAttributesSize
			}

			itemsBytes, newErroredItems := optimization.PackIntoSQSBatches(group.Messages, messageBodySizeThresholds)
			for _, newErroredItem := range newErroredItems {
				erroredItems = append(erroredItems, qbm_types.FlushErroredMessage{
					QueueName:         queue.QueueReference.Name,
					MessageBody:       newErroredItem.Content,
					MessageAttributes: group.MessageAttributes,
					MessageGroupID:    group.MessageGroupID,
					Err:               newErroredItem.Err,
				})
			}

			for _, itemBytes := range itemsBytes {
				entry := types.SendMessageBatchRequestEntry{
					Id:          aws.String(strconv.Itoa(len(sendMessageBatchRequestEntries))),
					MessageBody: aws.String(string(itemBytes)),
				}
				if !messageAttributes.IsEmpty() {
					entry.MessageAttributes = messageAttributes
				}
				if group.MessageGroupID != nil {
					entry.MessageGroupId = aws.String(*group.MessageGroupID)
				}
				sendMessageBatchRequestEntries = append(sendMessageBatchRequestEntries, entry)
			}

		}

		entryPackes, packingErrors := optimization.PackMessagesIntoRequests(
			sendMessageBatchRequestEntries,
			constants.AWSSendMessageBatchMaxTotalPayloadSize,
			constants.AWSSendMessageBatchMaxMessagesCount,
		)
		for _, packingError := range packingErrors {
			messageBody := map[string]any{}
			if packingError.Message.MessageBody != nil {
				_ = optimization.UnmarshalMessageBody([]byte(*packingError.Message.MessageBody), &messageBody)
			}
			messageAttributes := map[string]any{}
			if len(packingError.Message.MessageAttributes) > 0 {
				messageAttributes = entities.MessageAttributes(packingError.Message.MessageAttributes).ToMap()
			}
			erroredItems = append(erroredItems, qbm_types.FlushErroredMessage{
				QueueName:         queue.QueueReference.Name,
				MessageBody:       messageBody,
				MessageAttributes: messageAttributes,
				MessageGroupID:    packingError.Message.MessageGroupId,
				Err:               packingError.Err,
			})
		}

		erroredItems = append(erroredItems, qbm.sendBatchesConcurrently(entryPackes, sendMessageBatchRequestEntries, queue)...)

	}

	return qbm_types.FlushOutput{
		Errors: erroredItems,
	}
}

func (qbm *QBMProducer) sendBatchesConcurrently(
	entryPacks [][]types.SendMessageBatchRequestEntry,
	allEntries []types.SendMessageBatchRequestEntry,
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

			batchErrors := qbm.processSingleBatch(entries, allEntries, queue)

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
	allEntries []types.SendMessageBatchRequestEntry,
	queue Queue,
) []qbm_types.FlushErroredMessage {
	var batchErrors []qbm_types.FlushErroredMessage

	sendMessageBatchOutput, err := qbm.sqsClient.SendMessageBatch(qbm.ctx, &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(queue.QueueReference.URL),
		Entries:  entries,
	})
	if err != nil {
		for _, entry := range entries {
			var messageBody map[string]any
			if err = optimization.UnmarshalMessageBody([]byte(*entry.MessageBody), &messageBody); err != nil {
				messageBody = map[string]any{
					"original_message_body": *entry.MessageBody,
				}
			}
			var messageAttributes map[string]any
			if entry.MessageAttributes != nil {
				messageAttributes = make(map[string]any, len(entry.MessageAttributes))
				for key, value := range entry.MessageAttributes {
					messageAttributes[key] = value
				}
			}

			batchErrors = append(batchErrors, qbm_types.FlushErroredMessage{
				QueueName:         queue.QueueReference.Name,
				MessageBody:       messageBody,
				MessageAttributes: messageAttributes,
				MessageGroupID:    entry.MessageGroupId,
				Err:               err,
			})
		}
		return batchErrors
	}

	for _, failed := range sendMessageBatchOutput.Failed {
		internalID := ""
		if failed.Id != nil {
			internalID = *failed.Id
		} else {
			batchErrors = append(batchErrors, qbm_types.FlushErroredMessage{
				QueueName: queue.QueueReference.Name,
				Err:       internal_errors.NewSQSDeliveryError(false, "unknown", failed.SenderFault, failed.Message),
			})
			continue
		}
		parsedInternalID, err := strconv.Atoi(internalID)
		if err != nil {
			batchErrors = append(batchErrors, qbm_types.FlushErroredMessage{
				QueueName: queue.QueueReference.Name,
				Err:       internal_errors.NewSQSDeliveryError(false, "unknown", failed.SenderFault, failed.Message),
			})
			continue
		}
		failedItem := allEntries[parsedInternalID]
		code := "unknown"
		if failed.Code != nil {
			code = *failed.Code
		}
		messageBody := map[string]any{}
		if failedItem.MessageBody != nil {
			_ = optimization.UnmarshalMessageBody([]byte(*failedItem.MessageBody), &messageBody)
		}
		messageAttributes := map[string]any{}
		if len(failedItem.MessageAttributes) > 0 {
			messageAttributes = entities.MessageAttributes(failedItem.MessageAttributes).ToMap()
		}
		batchErrors = append(batchErrors, qbm_types.FlushErroredMessage{
			QueueName:         queue.QueueReference.Name,
			MessageBody:       messageBody,
			MessageAttributes: messageAttributes,
			MessageGroupID:    failedItem.MessageGroupId,
			Err:               internal_errors.NewSQSDeliveryError(parsedInternalID > 0, code, failed.SenderFault, failed.Message),
		})
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
