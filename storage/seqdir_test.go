package storage_test

import (
	"testing"

	"github.com/pjvds/seqcask/storage"
	"github.com/stretchr/testify/assert"
)

func BenchmarkSeqDirAdds(b *testing.B) {
	seqdir := storage.NewSeqDir()

	b.StartTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		seqdir.Add(uint64(iteration), 0, 0)
	}
	b.StopTimer()
}

func TestAddGet(t *testing.T) {
	seqdir := storage.NewSeqDir()
	sequence := uint64(0)
	valueSize := uint32(12)
	position := int64(88)

	seqdir.Add(sequence, valueSize, position)

	item, ok := seqdir.Get(sequence)

	assert.True(t, ok)
	assert.Equal(t, sequence, item.Sequence)
	assert.Equal(t, valueSize, item.ValueSize)
	assert.Equal(t, position, item.Position)
}

func TestAddAllGetAll(t *testing.T) {
	seqdir := storage.NewSeqDir()

	sequence := uint64(0)
	valueSize := uint32(12)

	items := make([]storage.Item, 255, 255)
	for index := range items {
		items[index].Sequence = uint64(index)
		items[index].ValueSize = valueSize
		items[index].Position = int64(index) * int64(valueSize)
	}

	seqdir.AddAll(items...)

	getItems := seqdir.GetAll(sequence, len(items))

	assert.Len(t, getItems, len(items))

	for index := range getItems {
		assert.Equal(t, getItems[index], items[index])
	}
}

func TestGetNonExisting(t *testing.T) {
	seqdir := storage.NewSeqDir()
	_, ok := seqdir.Get(0)

	assert.False(t, ok)
}
