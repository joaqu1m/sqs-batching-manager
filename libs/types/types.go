package types

type QueueReference struct {
	Name string
	URL  string
}

type AddMessageInput struct {
	QueueName         string
	MessageBody       map[string]any
	MessageAttributes map[string]any
	MessageGroupID    *string
}

type FlushOutput struct {
	Errors []FlushErroredMessage
}

type FlushErroredMessage struct {
	QueueName         string
	MessageBody       map[string]any
	MessageAttributes map[string]any
	MessageGroupID    *string
	Err               error
}
