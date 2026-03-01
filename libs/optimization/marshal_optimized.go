package optimization

import (
	"bytes"

	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"
)

func MarshalOptimized(data any) ([]byte, error) {
	// 1. Converter o dado (struct ou map) para MessagePack (Binário)
	// Se 'data' já for um []byte de um JSON, o msgpack vai apenas
	// encapsular o binário. O ideal é passar a struct original aqui.
	msgBinaria, err := msgpack.Marshal(data)
	if err != nil {
		return nil, err
	}

	// 2. Comprimir com Zstd
	// Criamos um buffer para receber o resultado
	var buf bytes.Buffer

	// Criamos o encoder (o nível 3 que você queria é o zstd.SpeedFastest ou similar)
	encoder, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}

	// Escreve os dados e fecha o encoder para dar flush no buffer
	_, err = encoder.Write(msgBinaria)
	if err != nil {
		encoder.Close()
		return nil, err
	}
	encoder.Close()

	return buf.Bytes(), nil
}

func UnmarshalOptimized(compressedData []byte, v any) error {
	// 1. Descomprimir com Zstd
	decoder, err := zstd.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return err
	}
	defer decoder.Close()

	decompressedData, err := decoder.DecodeAll(compressedData, nil)
	if err != nil {
		return err
	}

	// 2. Converter de MessagePack para a struct/map original
	return msgpack.Unmarshal(decompressedData, v)
}
