package seqcask

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/OneOfOne/xxhash"
)

type Batch struct {
	buffer    *bytes.Buffer
	positions []int

	done chan BatchWriteResult
}

// Puts a single value to this batch.
func (this *Batch) Put(value []byte) {
	startPosition := this.buffer.Len()
	// store the current item position
	this.positions = append(this.positions, startPosition)

	// write offset
	if err := binary.Write(this.buffer, binary.LittleEndian, uint64(0)); err != nil {
		panic(err)
	}
	// write value size
	if err := binary.Write(this.buffer, binary.LittleEndian, uint16(len(value))); err != nil {
		panic(err)
	}
	// write value
	if _, err := this.buffer.Write(value); err != nil {
		panic(err)
	}

	// get the slice of the buffer to read directly
	// from it without effecting the buffer state.
	rawBuffer := this.buffer.Bytes()
	itemData := rawBuffer[startPosition:]
	checksum := xxhash.Checksum64(itemData)

	// write checksum
	if err := binary.Write(this.buffer, binary.LittleEndian, checksum); err != nil {
		panic(err)
	}
}

// Reset truncates the buffer and positions
func (this *Batch) Reset() {
	this.buffer.Reset()
	this.positions = this.positions[0:0]
}

// WriteTo writes the content of the current batch
func (this *Batch) Write(startOffset uint64, writer io.Writer) (n int64, err error) {
	this.setOffsets(startOffset)
	n, err = this.buffer.WriteTo(writer)
	return
}

func (this *Batch) setOffsets(startOffset uint64) {
	bytes := this.buffer.Bytes()
	for index, position := range this.positions {
		binary.LittleEndian.PutUint64(bytes[position:], startOffset+uint64(index))
	}
}

func (this *Batch) Len() int {
	return len(this.positions)
}
