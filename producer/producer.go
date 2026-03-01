package producer

import (
	"context"
	"reflect"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/joaqu1m/sqs-batching-manager/libs/constants"
	"github.com/joaqu1m/sqs-batching-manager/libs/internal_errors"
	"github.com/joaqu1m/sqs-batching-manager/libs/optimization"
	qbm_types "github.com/joaqu1m/sqs-batching-manager/libs/types"
)

// Here we'll group items by 3 factors, to help the Flush method to decide how to batch them:

// 1. By queue, since we can't batch items from different queues together anyways
// 2. By the "batchable" factor, since if some consumer can't batch, we need to send its items one by one; That's determined by 2 properties:
//   - The presence of "BatchableConsumers" in the Queue struct, which represents the Consumer capacity to batch or not.
//   - The presence of "MessageAttributes" in the QueueItem struct, which means this item needs to have his own-and-solo MessageAttributes field
// 3. By the "MessageGroupID" field, since messages with different MessageGroupIDs can't be in the same batch.

type Queue struct {
	QueueReference                 qbm_types.QueueReference
	SoloQueueMessages              []SoloQueueMessage
	BatchableQueueMessagesByGroups map[string][]map[string]any
}

type SoloQueueMessage struct {
	MessageBody       map[string]any
	MessageAttributes map[string]any
	MessageGroupID    *string
}

type QBMProducer struct {
	ctx             context.Context
	sqsClient       *sqs.Client
	queues          map[string]Queue
	referenceQueues map[string]qbm_types.QueueReference
}

func NewQueueBatchingManagerProducer(ctx context.Context, sqsClient *sqs.Client, referenceQueues map[string]qbm_types.QueueReference) *QBMProducer {
	return &QBMProducer{
		ctx:             ctx,
		sqsClient:       sqsClient,
		queues:          map[string]Queue{},
		referenceQueues: referenceQueues,
	}
}

func (qbm *QBMProducer) Add(addMessageInput qbm_types.AddMessageInput) {

	if len(addMessageInput.MessageBody) == 0 {
		return
	}

	qbm.EnsureQueueExists(addMessageInput.QueueName)

	queue := qbm.queues[addMessageInput.QueueName]

	// if addMessageInput

	qbm.queues[addMessageInput.QueueName] = queue
}

func (qbm *QBMProducer) Flush() qbm_types.FlushResult {
	erroredItems := []qbm_types.FlushErroredMessage{}

	for _, queue := range qbm.queues {

		sendMessageBatchRequestEntries := []types.SendMessageBatchRequestEntry{}

		for messageGroupID, messages := range queue.BatchableQueueMessagesByGroups {

			itemsBytes, newErroredItems := optimization.PackIntoSQSBatches(messages, constants.AWSChargingThresholdsKiB)
			newFlushErroredItems := make([]qbm_types.FlushErroredMessage, len(newErroredItems))
			for i, newErroredItem := range newErroredItems {
				newFlushErroredItems[i] = qbm_types.FlushErroredMessage{
					QueueName:         queue.QueueReference.Name,
					MessageBody:       newErroredItem.Content,
					MessageAttributes: nil,
					MessageGroupID:    aws.String(messageGroupID),
					Err:               newErroredItem.Err,
				}
			}
			erroredItems = append(erroredItems, newFlushErroredItems...)

			if len(itemsBytes) == 0 {
				continue
			}

			for i, itemBytes := range itemsBytes {
				sendMessageBatchRequestEntries[i] = types.SendMessageBatchRequestEntry{
					Id:             aws.String(strconv.Itoa(i)),
					MessageBody:    aws.String(string(itemBytes)),
					MessageGroupId: aws.String(messageGroupID),
				}
			}

		}

		for _, soloMessage := range queue.SoloQueueMessages {

			var messageBody any = soloMessage.MessageBody
			if queue.QueueReference.BatchableConsumers {
				messageBody = []map[string]any{soloMessage.MessageBody}
			}

			itemBytes, err := optimization.Marshal(messageBody)
			if err != nil {
				erroredItems = append(erroredItems, qbm_types.FlushErroredMessage{
					QueueName:         queue.QueueReference.Name,
					MessageBody:       soloMessage.MessageBody,
					MessageAttributes: soloMessage.MessageAttributes,
					MessageGroupID:    soloMessage.MessageGroupID,
					Err:               internal_errors.WrapSerializationError(err),
				})
				continue
			}

			sendMessageBatchrequestEntry := types.SendMessageBatchRequestEntry{
				Id:          aws.String(strconv.Itoa(len(sendMessageBatchRequestEntries))),
				MessageBody: aws.String(string(itemBytes)),
			}

			messageAttributes := toMessageAttributes(soloMessage.MessageAttributes)
			if len(messageAttributes) > 0 {
				sendMessageBatchrequestEntry.MessageAttributes = messageAttributes
			}

			if soloMessage.MessageGroupID != nil {
				sendMessageBatchrequestEntry.MessageGroupId = soloMessage.MessageGroupID
			}

			sendMessageBatchRequestEntries = append(sendMessageBatchRequestEntries, sendMessageBatchrequestEntry)

		}

		entryPackes, packingErrors := optimization.PackMessagesIntoRequests(
			sendMessageBatchRequestEntries,
			constants.AWSSendMessageBatchMaxTotalPayloadSizeKiB,
			constants.AWSSendMessageBatchMaxMessagesCount,
		)
		if packingErrors != nil {
			// emit an error
		}

		for _, entries := range entryPackes {

			sendMessageBatchOutput, err := qbm.sqsClient.SendMessageBatch(qbm.ctx, &sqs.SendMessageBatchInput{
				QueueUrl: aws.String(queue.QueueReference.URL),
				Entries:  entries,
			})
			if err != nil {
				for _, entry := range entries {
					var messageBody map[string]any
					if err = optimization.Unmarshal([]byte(*entry.MessageBody), &messageBody); err != nil {
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
				code := "unknown"
				if failed.Code != nil {
					code = *failed.Code
				}
				erroredItems = append(erroredItems, qbm_types.FlushErroredMessage{
					Err: internal_errors.NewSQSDeliveryError(parsedInternalID > 0, code, failed.SenderFault, failed.Message),
					// let's fix this error payload soon
				})
			}

		}

	}

	qbm.queues = map[string]Queue{}

	return qbm_types.FlushResult{
		Errors: erroredItems,
	}
}

func (qbm *QBMProducer) EnsureQueueExists(queueName string) {

	_, alreadyExists := qbm.queues[queueName]
	if alreadyExists {
		return
	}

	queueReference, exists := qbm.referenceQueues[queueName]
	if !exists {
		qbm.referenceQueues[queueName] = qbm_types.QueueReference{
			Name:               queueName,
			URL:                qbm.FetchQueueURL(queueName),
			BatchableConsumers: false, // as it is not even in the queues map, we can assume it doesn't have batchable consumers
		}
		return
	}

	qbm.queues[queueName] = Queue{
		QueueReference:                 queueReference,
		SoloQueueMessages:              []SoloQueueMessage{},
		BatchableQueueMessagesByGroups: map[string][]map[string]any{},
	}

}

func (qbm *QBMProducer) FetchQueueURL(queueName string) string {

	getQueueURLOutput, err := qbm.sqsClient.GetQueueUrl(qbm.ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err == nil {
		if getQueueURLOutput.QueueUrl != nil {
			return *getQueueURLOutput.QueueUrl
		}
	}

	// If we can't fetch the URL, just parse the QueueName. For some reason, SQS allows queue names to be used as URLs in the SendMessageBatch API, so we can fallback to that.
	return queueName

}

func toMessageAttributes(messageAttributes map[string]any) map[string]types.MessageAttributeValue {

	result := make(map[string]types.MessageAttributeValue, len(messageAttributes))
	for key, value := range messageAttributes {

		reflectedValue := reflect.ValueOf(value)

		switch reflectedValue.Kind() {
		case reflect.String:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(reflectedValue.String()),
			}
		case reflect.Bool:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(strconv.FormatBool(reflectedValue.Bool())),
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.FormatInt(reflectedValue.Int(), 10)),
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.FormatUint(reflectedValue.Uint(), 10)),
			}
		case reflect.Float32, reflect.Float64:
			result[key] = types.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.FormatFloat(reflectedValue.Float(), 'f', -1, 64)),
			}
		}

	}

	return result
}
