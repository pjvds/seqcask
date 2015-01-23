package seqcask

import "sync"

type SeqDir struct {
	shards     []map[uint64]Item
	shardSize  int
	shardLocks []sync.RWMutex
}

type Item struct {
	FileId    uint16
	ValueSize uint16
	Position  int64
}

func NewSeqDir() *SeqDir {
	shards := make([]map[uint64]Item, 256)
	for i := 0; i < len(shards); i++ {
		shards[i] = make(map[uint64]Item, 1024)
	}

	return &SeqDir{
		shards:     shards,
		shardLocks: make([]sync.RWMutex, 256, 256),
		shardSize:  256,
	}
}

func (this *SeqDir) getShard(seq uint64) (map[uint64]Item, sync.RWMutex) {
	index := int(seq % uint64(this.shardSize))
	return this.shards[index], this.shardLocks[index]
}

func (this *SeqDir) Add(seq uint64, fid, valueSize uint16, position int64) {
	shard, lock := this.getShard(seq)

	lock.Lock()
	shard[seq] = Item{fid, valueSize, position}
	lock.Unlock()
}

func (this *SeqDir) Get(offset uint64) (*Item, bool) {
	shard, lock := this.getShard(offset)

	lock.RLock()
	item, ok := shard[offset]
	lock.RUnlock()

	return &item, ok
}
