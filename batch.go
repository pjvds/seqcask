package seqcask

import (
	"bytes"
	"errors"

	"github.com/OneOfOne/xxhash"
)

type WriteBatch struct {
	buffer     bytes.Buffer
	positions  []int
	valueSizes []uint32

	// used to store the seqdir items while
	// writing to append them all at once
	itemBuffer []Item

	writeErr      error
	writePosition int64
	writeSequence uint64

	writeDone chan struct{}
}

func NewWriteBatch() *WriteBatch {
	batch := new(WriteBatch)
	batch.writeDone = make(chan struct{}, 1)

	return batch
}

func (this *WriteBatch) getSeqdirItems() []Item {
	msgCount := this.Len()
	// make sure we have enought capacity in the item slice
	// we only care about capacity, not about the content so
	// recreating it is not a problem at all
	if len(this.itemBuffer) < msgCount {
		this.itemBuffer = make([]Item, msgCount, msgCount)
	}

	// create seqdir items for every message
	for index := 0; index < msgCount; index++ {
		this.itemBuffer[index] = Item{
			FileId:    0, // TODO: set
			ValueSize: this.valueSizes[index],
			Position:  this.writePosition + int64(this.positions[index]),
		}
	}

	return this.itemBuffer[0:msgCount]
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
	this.writePosition = 0
	this.writeSequence = 0
	this.writeErr = errors.New("write never started")
}

func (this *WriteBatch) Bytes() []byte {
	return this.buffer.Bytes()
}

func (this *WriteBatch) Len() int {
	return len(this.positions)
}
