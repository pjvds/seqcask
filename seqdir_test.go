package seqcask

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkSeqDirAdds(b *testing.B) {
	seqdir := NewSeqDir()

	b.StartTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		seqdir.Add(uint64(iteration), 0, 0, 0)
	}
	b.StopTimer()
}

func TestAddGet(t *testing.T) {
	seqdir := NewSeqDir()
	sequence := uint64(0)
	fileId := uint16(6)
	valueSize := uint32(12)
	position := int64(88)

	seqdir.Add(sequence, fileId, valueSize, position)

	item, ok := seqdir.Get(sequence)

	assert.True(t, ok)
	assert.Equal(t, item.FileId, fileId)
	assert.Equal(t, valueSize, item.ValueSize)
	assert.Equal(t, position, item.Position)
}

func TestAddAllGetAll(t *testing.T) {
	seqdir := NewSeqDir()

	sequence := uint64(0)
	fileId := uint16(1)
	valueSize := uint32(12)

	items := make([]Item, 255, 255)
	for index, item := range items {
		item.FileId = fileId
		item.ValueSize = valueSize
		item.Position = int64(index) * int64(valueSize)
	}

	seqdir.AddAll(sequence, 0, items...)

	getItems := seqdir.GetAll(sequence, len(items))

	assert.Len(t, getItems, len(items))

	for index := range getItems {
		assert.Equal(t, getItems[index], items[index])
	}
}

func TestGetNonExisting(t *testing.T) {
	seqdir := NewSeqDir()
	_, ok := seqdir.Get(0)

	assert.False(t, ok)
}
