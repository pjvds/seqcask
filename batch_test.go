package seqcask_test

import (
	"testing"

	"github.com/pjvds/seqcask"
	"github.com/stretchr/testify/assert"
)

func TestNewWriteBatch(t *testing.T) {
	batch := seqcask.NewWriteBatch()
	assert.NotNil(t, batch)
}

func TestWrite(t *testing.T) {
	batch := seqcask.NewWriteBatch()
	value := []byte("foobar")
	batch.Put(value)

	buffer := batch.Bytes()

	// length should be:
	// value size (uint32) + value + checksum (uint64)
	assert.Equal(t, 4+len(value)+8, len(buffer))
}

func BenchmarkPut1mbOf200BytesMessages(b *testing.B) {
	batch := seqcask.NewWriteBatch()
	// 5000 * 200 bytes values = 1 megabyte
	messages := make([][]byte, 5000, 5000)

	for index := range messages {
		messages[index] = RandomValue(200)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.SetBytes(5000 * 200)
		batch.Put(messages...)
		batch.Reset()
	}
}
