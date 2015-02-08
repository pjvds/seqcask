package storage_test

import (
	"testing"

	"github.com/pjvds/randombytes"
	"github.com/pjvds/seqcask/storage"
	"github.com/stretchr/testify/assert"
)

func TestNewWriteBatch(t *testing.T) {
	batch := storage.NewWriteBatch()
	assert.NotNil(t, batch)
}

func TestWrite(t *testing.T) {
	batch := storage.NewWriteBatch()
	value := []byte("foobar")
	batch.Put(value)

	buffer := batch.Bytes()

	// length should be:
	// value size (uint32) + value + checksum (uint64)
	assert.Equal(t, 4+len(value)+8, len(buffer))
}

func BenchmarkPut1mbOf200BytesMessages(b *testing.B) {
	batch := storage.NewWriteBatch()
	// 5000 * 200 bytes values = 1 megabyte
	messages := make([][]byte, 5000, 5000)

	for index := range messages {
		messages[index] = randombytes.Make(200)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.SetBytes(5000 * 200)
		batch.Put(messages...)
		batch.Reset()
	}
}
