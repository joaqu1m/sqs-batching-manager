package constants

// All of this values are described here: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html

var (
	AWSSQSChargingThresholds = []uint64{
		64 * 1024,  // 64 KiB
		128 * 1024, // 128 KiB
		192 * 1024, // 192 KiB
		256 * 1024, // 256 KiB
	}
)

const (
	AWSSendMessageBatchMaxTotalPayloadSize uint64 = 1024 * 1024 // 1 MiB
	AWSSendMessageBatchMaxMessagesCount    uint64 = 10
)
