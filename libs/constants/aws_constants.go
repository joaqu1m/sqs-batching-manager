package constants

// All of this values are described here: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html

var (
	AWSSQSChargingThresholds = []uint64{
		64 * 1 * 1024,  // 64 KiB
		64 * 2 * 1024,  // 128 KiB
		64 * 3 * 1024,  // 192 KiB
		64 * 4 * 1024,  // 256 KiB
		64 * 5 * 1024,  // 320 KiB
		64 * 6 * 1024,  // 384 KiB
		64 * 7 * 1024,  // 448 KiB
		64 * 8 * 1024,  // 512 KiB
		64 * 9 * 1024,  // 576 KiB
		64 * 10 * 1024, // 640 KiB
		64 * 11 * 1024, // 704 KiB
		64 * 12 * 1024, // 768 KiB
		64 * 13 * 1024, // 832 KiB
		64 * 14 * 1024, // 896 KiB
		64 * 15 * 1024, // 960 KiB
		64 * 16 * 1024, // 1 MiB
	}
)

const (
	AWSSendMessageBatchMaxTotalPayloadSize uint64 = 1024 * 1024 // 1 MiB
	AWSSendMessageBatchMaxMessagesCount    uint64 = 10
)
