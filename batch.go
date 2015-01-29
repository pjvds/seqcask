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
	itemCount  int
}

func NewWriteBatch() *WriteBatch {
	batch := new(WriteBatch)
	return batch
}

// Get the seqdir items.
// This method may only called *ONCE* after a write was successfull
// because it updates the item positions.
func (this *WriteBatch) getSeqdirItems(sequence uint64, position int64) []Item {

	// the positions we currently have are set to the position
	// in the buffer, which means the first item is 0, 2nd item
	// is 0 + first item size, etc.
	for index := 0; index < this.Len(); index++ {
		this.itemBuffer[index].Sequence = sequence + uint64(index)
		this.itemBuffer[index].Position += position
	}

	return this.itemBuffer[0:this.itemCount]
}

// Puts a single value to this WriteBatch.
func (this *WriteBatch) Put(values ...[]byte) {
	// Grow the item buffer if needed.
	if len(this.itemBuffer) < this.itemCount+len(values) {
		size := len(this.itemBuffer) + (len(values) * 2)
		largerItemBuffer := make([]Item, size, size)
		copy(largerItemBuffer, this.itemBuffer)

		this.itemBuffer = largerItemBuffer
	}

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
		// the correct position is set when getSeqdirItems is called
		this.itemBuffer[this.itemCount] = Item{
			ValueSize: valueSize,
			Position:  int64(startPosition),
		}

		this.itemCount++
	}
}

// Reset truncates the buffer and positions
func (this *WriteBatch) Reset() {
	this.buffer.Reset()
	this.itemCount = 0
}

func (this *WriteBatch) Bytes() []byte {
	return this.buffer.Bytes()
}

func (this *WriteBatch) Len() int {
	return this.itemCount
}
