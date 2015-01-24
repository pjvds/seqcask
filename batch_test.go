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
