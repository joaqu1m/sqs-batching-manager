package constants

// All of this values are described here: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html

const (
	AWSSendMessageBatchMaxTotalPayloadSizeKiB uint64 = 1024
	AWSSendMessageBatchMaxMessagesCount       uint64 = 10
)

var (
	AWSChargingThresholdsKiB = []uint64{64, 128, 192, 256}
)
