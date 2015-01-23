package seqcask

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/OneOfOne/xxhash"
)

type WriteBatch struct {
	buffer    *bytes.Buffer
	positions []int

	done chan BatchWriteResult
}

func NewWriteBatch() *WriteBatch {
	batch := &WriteBatch{
		buffer:    new(bytes.Buffer),
		positions: make([]int, 0, 256),

		done: make(chan BatchWriteResult, 1),
	}
	batch.buffer.Grow(5 * 1000 * 1024) // 5MB
	return batch
}

// Puts a single value to this WriteBatch.
func (this *WriteBatch) Put(values ...[]byte) {
	for _, value := range values {
		startPosition := this.buffer.Len()
		// store the current item position
		this.positions = append(this.positions, startPosition)

		// write value size
		binary.Write(this.buffer, binary.LittleEndian, uint32(len(value)))

		// write value
		this.buffer.Write(value)

		// create hash from value only, offset value
		// should always be atomicly increasing and
		// therefor can be verified. if the value size
		// is of the checksum should fail.
		checksum := xxhash.Checksum64(value)

		// write checksum
		binary.Write(this.buffer, binary.LittleEndian, checksum)
	}
}

// Reset truncates the buffer and positions
func (this *WriteBatch) Reset() {
	this.buffer.Reset()
	this.positions = this.positions[0:0]
}

// WriteTo writes the content of the current WriteBatch
func (this *WriteBatch) WriteTo(writer io.Writer) (n int, err error) {
	return writer.Write(this.buffer.Bytes())
}

func (this *WriteBatch) Len() int {
	return len(this.positions)
}
