# SQS Batching Manager — Internal Documentation

## Cost Model & Motivation

AWS SQS charges per message in multiples of 64 KiB, up to 1 MiB. A 1-byte message and a 64 KiB message cost the same. The core strategy to exploit this is **double-batching**: instead of sending one message per SQS entry, each entry carries an array of messages packed into a single `MessageBody`. This means N messages can be charged as a single billing unit, as long as they fit within a threshold.

The trade-off is giving up AWS's native per-message retry and the ability to set unique `MessageGroupID` / `MessageAttributes` per logical message. Both are handled explicitly — attributes and group IDs are shared per batch group, and retry responsibility moves to the consumer.

---

## Serialization

Message bodies (arrays of `map[string]any`) are serialized with **MessagePack + Zstd** compression (`MarshalOptimized` / `UnmarshalOptimized`). This reduces byte size significantly compared to plain JSON, directly impacting how many messages fit within each billing threshold.

`MarshalMessageBody` / `UnmarshalMessageBody` are the internal wrappers used throughout the packing pipeline. The compression behavior is toggled by the `COMPRESS_QUEUE_VALUES` constant.

---

## Grouping Strategy

Messages are buffered and grouped by a composite key of `(MessageGroupID, marshaledMessageAttributes)`. This is necessary because a single SQS entry can only carry one `MessageGroupId` and one set of `MessageAttributes` — both will apply uniformly to all logical messages packed inside that entry. The consumer must understand this convention.

Groups are scoped per queue. The key type is `groupKey`, and each group holds the shared attributes and an accumulated slice of message bodies.

---

## Packing Pipeline (Flush)

`Flush()` is always manual and processes the full buffer. The pipeline has two FFD stages:

### Stage 1 — Item Packing (`PackIntoSQSBatches`)

Within each group, individual messages are packed into `SendMessageBatchRequestEntry` bodies using a variant of **First Fit Decreasing (FFD)**:

1. Each message is serialized alone to check its size. Messages that already exceed the maximum threshold alone are immediately errored out.
2. Remaining items are sorted descending by size.
3. FFD assigns each item to the first batch where it fits within the **charging threshold ceiling** — the smallest threshold ≥ current batch size. This means the packing tries to fill batches up to the cheapest tier that covers them (64 KiB, 128 KiB, 192 KiB, or 256 KiB).

The `MessageAttributes` size is **subtracted from all thresholds** before this step, since attributes occupy space in every entry that shares them.

SQS attribute size is calculated as the sum of: `len(name) + len(dataType) + len(value)` for each attribute — matching AWS's own billing formula.

### Stage 2 — Request Packing (`PackMessagesIntoRequests`)

The resulting `SendMessageBatchRequestEntry` list is then packed into individual `SendMessageBatch` calls, again with FFD, respecting two AWS hard limits:
- Max **10 entries** per request
- Max **1 MiB** total payload per request (sum of each entry's `MessageBody` + `MessageAttributes` size)

Entries that exceed the 1 MiB limit on their own are errored out immediately.

---

## Auto-Flush

When the internal buffer reaches **50 MiB** of accumulated message data, an auto-flush is triggered automatically (called from within `Add`). The result is not returned to the caller — errors are stored in `failedDelayedMessages` and prepended to the `Errors` slice of the next `Flush()` call. A manual `Flush()` is always required at shutdown.

The 50 MiB threshold is approximate; it is computed from raw message bytes and serialized attribute bytes, not from AWS billing sizes.

---

## Error Handling

Every item that fails at any stage — serialization error, size exceeded, packing error, AWS API error, or per-entry failure reported by `SendMessageBatch.Failed` — is captured as a `FlushErroredMessage` and returned in `FlushOutput.Errors`. The original message body, attributes, group ID, queue name, and the underlying error are all preserved. No errors are silently dropped.

---

## Consumer (Pending)

The consumer is not yet implemented. Key open points include: decoding the packed `MessageBody` array, per-message retry logic (replacing the native AWS mechanism lost due to double-batching), and correctly interpreting the shared `MessageGroupId` / `MessageAttributes` as applying to all logical messages in the entry.
