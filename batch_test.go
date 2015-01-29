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
	typeId := uint16(3)
	partitionKey := uint16(19)

	message := seqcask.NewMessage(typeId, partitionKey, []byte("foobar"))
	batch.Put(message)

	buffer := batch.Bytes()

	// length should be:
	// value size (uint32) + value + checksum (uint64)
	assert.Equal(t, 4+len(message.Value)+8, len(buffer))
}

func BenchmarkPut1mbOf200BytesMessages(b *testing.B) {
	batch := seqcask.NewWriteBatch()
	// 5000 * 200 bytes values = 1 megabyte
	messages := make([]seqcask.Message, 5000, 5000)

	for index := range messages {
		typeId := uint16(3)
		partitionKey := uint16(19)
		messages[index] = seqcask.NewMessage(typeId, partitionKey, RandomValue(200))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.SetBytes(5000 * 200)
		batch.Put(messages...)
		batch.Reset()
	}
}
