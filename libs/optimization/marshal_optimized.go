package optimization

import (
	"encoding/ascii85"

	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
)

// zstdEncoder and zstdDecoder are shared across calls.
// EncodeAll and DecodeAll are concurrency-safe and reuse internal state,
// which at SpeedBestCompression enables cross-call dictionary warmup — improving
// compression over time as the encoder sees more similar data.
var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	zstdDecoder, _ = zstd.NewReader(nil)
)

func MarshalOptimized(data any) ([]byte, error) {
	// 1. Serialize the data (struct or map) to MessagePack (binary)
	msgBytes, err := msgpack.Marshal(data)
	if err != nil {
		return nil, err
	}

	// 2. Compress with Zstd (shared encoder — EncodeAll is concurrency-safe)
	compressed := zstdEncoder.EncodeAll(msgBytes, make([]byte, 0, len(msgBytes)))

	// 3. Encode as ASCII85 to guarantee valid UTF-8 text (e.g. SQS MessageBody)
	dst := make([]byte, ascii85.MaxEncodedLen(len(compressed)))
	n := ascii85.Encode(dst, compressed)
	return dst[:n], nil
}

func UnmarshalOptimized(encodedData []byte, v any) error {
	// 1. Decode ASCII85
	dst := make([]byte, len(encodedData))
	ndst, _, err := ascii85.Decode(dst, encodedData, true)
	if err != nil {
		return err
	}

	// 2. Decompress with Zstd (shared decoder — DecodeAll is concurrency-safe)
	decompressed, err := zstdDecoder.DecodeAll(dst[:ndst], nil)
	if err != nil {
		return err
	}

	// 3. Deserialize from MessagePack back to the original struct/map
	return msgpack.Unmarshal(decompressed, v)
}
