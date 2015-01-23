package seqcask_test

import (
	"bytes"
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

	buffer := new(bytes.Buffer)
	batch.Write(0, buffer)

	// length should be:
	// offset (uint64) + value size (uint32) + value + checksum (uint64)
	assert.Equal(t, 8+4+len(value)+8, buffer.Len())
}
