package seqcask

import (
	"bytes"
	"io"

	"github.com/OneOfOne/xxhash"
)

type WriteBatch struct {
	buffer    bytes.Buffer
	positions []int

	done chan BatchWriteResult
}

func NewWriteBatch() *WriteBatch {
	batch := &WriteBatch{
		positions: make([]int, 0, 256),

		done: make(chan BatchWriteResult, 1),
	}
	batch.buffer.Grow(5 * 1000 * 1024) // 5MB
	return batch
}

// Puts a single value to this WriteBatch.
func (this *WriteBatch) Put(values ...[]byte) {
	for _, value := range values {
		// store the current item position
		startPosition := this.buffer.Len()
		this.positions = append(this.positions, startPosition)

		// write value size
		valueSize := uint32(len(value))
		this.buffer.Write([]byte{byte(valueSize >> 24), byte(valueSize >> 16), byte(valueSize >> 8), byte(valueSize >> 0)})

		// write value
		this.buffer.Write(value)

		// create checksum
		checksum := xxhash.Checksum64(value)

		// write checksum
		this.buffer.Write([]byte{byte(checksum >> 56), byte(checksum >> 48), byte(checksum >> 40), byte(checksum >> 32),
		    byte(checksum >> 24), byte(checksum >> 16), byte(checksum >> 8), byte(checksum >> 0)})
	}
}

// Reset truncates the buffer and positions
func (this *WriteBatch) Reset() {
	this.buffer.Reset()
	this.positions = this.positions[0:0]
}

func (this *WriteBatch) Bytes() []byte {
	return this.buffer.Bytes()
}

// WriteTo writes the content of the current WriteBatch
func (this *WriteBatch) WriteTo(writer io.Writer) (n int, err error) {
	return writer.Write(this.buffer.Bytes())
}

func (this *WriteBatch) Len() int {
	return len(this.positions)
}
