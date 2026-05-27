# SQS Batching Manager

using AWS SQS send message feature with maximum cost-efficiency

only standard queues are supported

## Usage

```go
import (
    "context"

    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    "github.com/joaqu1m/sqs-batching-manager/producer"
    qbm_types "github.com/joaqu1m/sqs-batching-manager/libs/types"
)

// process scope: build the AWS SDK client once
awsCfg, _ := config.LoadDefaultConfig(context.Background())
sqsClient := sqs.NewFromConfig(awsCfg)

// per work unit: build a producer, Add(), Flush()
queues := map[string]qbm_types.QueueReference{
    "orders": {Name: "orders", URL: "https://sqs.us-east-1.amazonaws.com/123/orders"},
}
p := producer.NewQueueBatchingManagerProducer(ctx, sqsClient, queues)

p.Add(qbm_types.AddMessageInput{
    QueueName:   "orders",
    MessageID:   "order-42",            // caller-provided; surfaces back on failure
    MessageBody: map[string]any{"orderId": 42, "total": 199.90},
})

out := p.Flush()
for _, errMsg := range out.Errors {
    // One record per failed MessageID. An entry-level failure that packed N logical messages fans out
    // into N records here, all sharing the same Err.
    log.Printf("queue=%s id=%s err=%v", errMsg.QueueName, errMsg.MessageID, errMsg.Err)
}
```

**Failures return IDs, not bodies.** The caller is the source of truth for `id → item`: keep your originals until `Flush()` returns successfully, otherwise you cannot retry. See *Lifecycle & concurrency* below.

## Lifecycle & concurrency

`QBMProducer` is stateful: messages and failure records live inside the instance from `Add()` until the next `Flush()`. Use **one `QBMProducer` per logical work unit** — one per Lambda invocation, one per HTTP request, one per batch job — and always `Flush()` before the work unit ends.

The underlying `*sqs.Client`, on the other hand, owns the connection pool and is safe for concurrent use. Build it **once** at process scope and pass it to every `QBMProducer` you create; do not rebuild it per invocation.

```go
// process scope — built once, shared by every producer
var sqsClient = sqs.NewFromConfig(awsCfg)

// inside a handler / work unit — fresh producer, Flush() before returning
func Handle(ctx context.Context, evt Event) error {
    producer := producer.NewQueueBatchingManagerProducer(ctx, sqsClient, queues)
    // ... producer.Add(...) ...
    out := producer.Flush()
    // inspect out.Errors
    return nil
}
```
