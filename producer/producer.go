package producer

import (
	"context"
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
	MessageGroupID    *string
	MessageAttributes map[string]any
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

	if len(addMessageInput.MessageBody) == 0 {
		return
	}

	qbm.mu.Lock()
	defer qbm.mu.Unlock()

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
		if attrsBytes, err := entities.NewMessageAttributesFromMap(addMessageInput.MessageAttributes).Marshal(); err == nil {
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
	qbm.queuesTotalSize += uint64(len(addMessageInput.MessageBody)) + uint64(len(marshaledAttrs))

	qbm.autoFlushIfNeeded()
}

const MAX_SIZE_BEFORE_FLUSHING = 1024 * 1024 * 50 // 50 MiB

func (qbm *QBMProducer) autoFlushIfNeeded() {

	if qbm.queuesTotalSize >= MAX_SIZE_BEFORE_FLUSHING {
		flushOutput := qbm.Flush()
		qbm.queuesTotalSize = 0
		qbm.failedDelayedMessages = append(qbm.failedDelayedMessages, flushOutput.Errors...)
	}

}

func (qbm *QBMProducer) Flush() qbm_types.FlushOutput {
	erroredItems := []qbm_types.FlushErroredMessage{}

	qbm.mu.Lock()
	queues := qbm.queues
	qbm.queues = map[string]Queue{}
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
			messageBodySizeThresholds := constants.AWSSQSChargingThresholds
			for i, threshold := range messageBodySizeThresholds {
				messageBodySizeThresholds[i] = threshold - uint64(messageAttributesSize)
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

		for _, entries := range entryPackes {

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

					erroredItems = append(erroredItems, qbm_types.FlushErroredMessage{
						QueueName:         queue.QueueReference.Name,
						MessageBody:       messageBody,
						MessageAttributes: messageAttributes,
						MessageGroupID:    entry.MessageGroupId,
						Err:               err,
					})
				}
				continue
			}
			for _, failed := range sendMessageBatchOutput.Failed {
				internalID := ""
				if failed.Id != nil {
					internalID = *failed.Id
				} else {
					erroredItems = append(erroredItems, qbm_types.FlushErroredMessage{
						QueueName: queue.QueueReference.Name,
						Err:       internal_errors.NewSQSDeliveryError(false, "unknown", failed.SenderFault, failed.Message),
					})
					continue
				}
				parsedInternalID, err := strconv.Atoi(internalID)
				if err != nil {
					erroredItems = append(erroredItems, qbm_types.FlushErroredMessage{
						QueueName: queue.QueueReference.Name,
						Err:       internal_errors.NewSQSDeliveryError(false, "unknown", failed.SenderFault, failed.Message),
					})
					continue
				}
				failedItem := sendMessageBatchRequestEntries[parsedInternalID]
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
				erroredItems = append(erroredItems, qbm_types.FlushErroredMessage{
					QueueName:         queue.QueueReference.Name,
					MessageBody:       messageBody,
					MessageAttributes: messageAttributes,
					MessageGroupID:    failedItem.MessageGroupId,
					Err:               internal_errors.NewSQSDeliveryError(parsedInternalID > 0, code, failed.SenderFault, failed.Message),
				})
			}

		}

	}

	return qbm_types.FlushOutput{
		Errors: erroredItems,
	}
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
