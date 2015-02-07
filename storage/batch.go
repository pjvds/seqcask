package seqcask

import (
	"bytes"

	"github.com/OneOfOne/xxhash"
)

type WriteBatch struct {
	buffer bytes.Buffer

	// used to store the seqdir items while
	// writing to append them all at once
	itemBuffer []Item
}

func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		itemBuffer: make([]Item, 0, 50),
	}
}

// Puts a single value to this WriteBatch.
func (this *WriteBatch) Put(messages ...[]byte) {
	startSequence := this.Len()

	// Why don't we check for errors when writing to the buffer?
	// This is because the error result is always nil. Write will
	// just write the bytes to memory and that should not fail.
	// The only thing that can go wrong is that the buffer becomes
	// too large. If that happens Write will panic with ErrTooLarge.

	for index, message := range messages {
		// store the current item position
		startPosition := this.buffer.Len()

		// write value size
		valueSize := uint32(len(message))
		this.buffer.Write([]byte{byte(valueSize >> 24), byte(valueSize >> 16), byte(valueSize >> 8), byte(valueSize >> 0)})

		// write value
		this.buffer.Write(message)

		// create checksum
		checksum := xxhash.Checksum64(message)

		// write checksum
		this.buffer.Write([]byte{byte(checksum >> 56), byte(checksum >> 48), byte(checksum >> 40), byte(checksum >> 32),
			byte(checksum >> 24), byte(checksum >> 16), byte(checksum >> 8), byte(checksum >> 0)})

		this.itemBuffer = append(this.itemBuffer, Item{
			Sequence:  uint64(startSequence + index),
			ValueSize: valueSize,
			Position:  int64(startPosition),
		})
	}
}

// Reset truncates the buffer and positions
func (this *WriteBatch) Reset() {
	this.buffer.Reset()
	this.itemBuffer = this.itemBuffer[0:0]
}

func (this *WriteBatch) Bytes() []byte {
	return this.buffer.Bytes()
}

func (this *WriteBatch) Len() int {
	return len(this.itemBuffer)
}
