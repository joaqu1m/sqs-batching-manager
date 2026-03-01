package types

type QueueReference struct {
	Name               string
	URL                string
	BatchableConsumers bool
}

type AddMessageInput struct {
	QueueName         string
	MessageBody       map[string]any
	MessageAttributes map[string]any
	MessageGroupID    *string
}

type FlushResult struct {
	Errors []FlushErroredMessage
}

type FlushErroredMessage struct {
	QueueName         string
	MessageBody       map[string]any
	MessageAttributes map[string]any
	MessageGroupID    *string
	Err               error
}
