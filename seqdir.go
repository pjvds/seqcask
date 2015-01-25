package seqcask

import "sync"

type SeqDir struct {
	items map[uint64]Item
	lock  sync.RWMutex
}

type Item struct {
	FileId    uint16
	ValueSize uint32
	Position  int64
}

func NewSeqDir() *SeqDir {
	return &SeqDir{
		items: make(map[uint64]Item, 1024),
	}
}

func (this *SeqDir) Add(seq uint64, fid uint16, valueSize uint32, position int64) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.items[seq] = Item{fid, valueSize, position}
}

func (this *SeqDir) AddAll(sequenceStart uint64, positionStart int64, items ...Item) {
	this.lock.Lock()
	defer this.lock.Unlock()

	for index, item := range items {
		item.Position += positionStart
		this.items[sequenceStart+uint64(index)] = item
	}
}

func (this *SeqDir) Get(offset uint64) (Item, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	item, ok := this.items[offset]
	return item, ok
}

func (this *SeqDir) GetAll(from uint64, length int) []Item {
	this.lock.RLock()
	defer this.lock.RUnlock()

	items := make([]Item, 0, length)
	to := from + uint64(length)

	for sequence := from; sequence < to; sequence++ {
		item, ok := this.items[sequence]

		if ok {
			items = append(items, item)
		} else {
			break
		}
	}

	return items
}
