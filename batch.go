package seqcask

import (
	"bytes"

	"github.com/OneOfOne/xxhash"
)

type Message struct {
	TypeId       uint16
	PartitionKey uint16
	Value        []byte
}

func NewMessage(typeId uint16, partitionKey uint16, value []byte) Message {
	return Message{
		TypeId:       typeId,
		PartitionKey: partitionKey,
		Value:        value,
	}
}

type WriteBatch struct {
	buffer bytes.Buffer

	messages         []Message
	messagePositions []int64

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
	items := make([]Item, len(this.messages), len(this.messages))

	for index, message := range this.messages {
		valueSize := len(message.Value)

		items[index] = Item{
			Sequence:     sequence + uint64(index),
			PartitionKey: message.PartitionKey,
			TypeId:       message.TypeId,
			ValueSize:    uint32(valueSize),
			Position:     position + this.messagePositions[index],
		}
	}

	return items
}

// Puts a single value to this WriteBatch.
func (this *WriteBatch) Put(messages ...Message) {
	for _, message := range messages {
		// store the current item position
		startPosition := this.buffer.Len()

		this.messages = append(this.messages, message)
		this.messagePositions = append(this.messagePositions, int64(startPosition))

		// write value size
		valueSize := uint32(len(message.Value))
		this.buffer.Write([]byte{byte(valueSize >> 24), byte(valueSize >> 16), byte(valueSize >> 8), byte(valueSize >> 0)})

		// write value
		this.buffer.Write(message.Value)

		// create checksum
		checksum := xxhash.Checksum64(message.Value)

		// write checksum
		this.buffer.Write([]byte{byte(checksum >> 56), byte(checksum >> 48), byte(checksum >> 40), byte(checksum >> 32),
			byte(checksum >> 24), byte(checksum >> 16), byte(checksum >> 8), byte(checksum >> 0)})
	}
}

// Reset truncates the buffer and positions
func (this *WriteBatch) Reset() {
	this.buffer.Reset()
	this.messages = this.messages[0:0]
	this.messagePositions = this.messagePositions[0:0]
}

func (this *WriteBatch) Bytes() []byte {
	return this.buffer.Bytes()
}

func (this *WriteBatch) Len() int {
	return this.itemCount
}
