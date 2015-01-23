package seqcask

import "sync"

type SeqDir struct {
	items map[uint64]Item
	lock  sync.RWMutex
}

type Item struct {
	FileId    uint16
	ValueSize uint16
	Position  int64
}

func NewSeqDir() *SeqDir {
	return &SeqDir{
		items: make(map[uint64]Item, 1024),
	}
}

func (this *SeqDir) Add(seq uint64, fid, valueSize uint16, position int64) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.items[seq] = Item{fid, valueSize, position}
}

func (this *SeqDir) AddAll(seqStart uint64, items ...Item) {
	this.lock.Lock()
	defer this.lock.Unlock()

	for index, item := range items {
		this.items[seqStart+uint64(index)] = item
	}
}

func (this *SeqDir) Get(offset uint64) (Item, bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	item, ok := this.items[offset]
	return item, ok
}
