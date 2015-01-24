package seqcask

import (
	"bytes"

	"github.com/OneOfOne/xxhash"
)

type WriteBatch struct {
	buffer     bytes.Buffer
	positions  []int
	valueSizes []uint32
}

func NewWriteBatch() *WriteBatch {
	return new(WriteBatch)
}

// Puts a single value to this WriteBatch.
func (this *WriteBatch) Put(values ...[]byte) {
	for _, value := range values {
		// store the current item position
		startPosition := this.buffer.Len()

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

		// store the relative start position of this value
		// this us used to calculate the file position
		// when adding the seqdir items after a successfull write
		this.positions = append(this.positions, startPosition)
		this.valueSizes = append(this.valueSizes, valueSize)
	}
}

// Reset truncates the buffer and positions
func (this *WriteBatch) Reset() {
	this.buffer.Reset()
	this.positions = this.positions[0:0]
	this.valueSizes = this.valueSizes[0:0]
}

func (this *WriteBatch) Bytes() []byte {
	return this.buffer.Bytes()
}

func (this *WriteBatch) Len() int {
	return len(this.positions)
}
