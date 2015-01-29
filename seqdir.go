package seqcask

import "sync"

type SeqDir struct {
	items map[uint64]Item
	lock  sync.RWMutex

	empty bool

	lastKey uint64
}

type Item struct {
	// The sequence number of the item.
	Sequence uint64

	// The partition key of the message, ranging 0-65535.
	// Every message that is stored is stored with a partition key. This value is
	// set by the one that puts the message in the store. This is usually done
	// by hashing the primary id of the message payload, for example the user id.
	PartitionKey uint16

	// The type id of the message that is stored.
	TypeId uint16

	// The size of the value in bytes.
	ValueSize uint32

	// Points to the position in the data file that holds the item.
	Position int64
}

func NewSeqDir() *SeqDir {
	return &SeqDir{
		items: make(map[uint64]Item, 1024),
		empty: true,
	}
}

func (this *SeqDir) Add(sequence uint64, partitionKey uint16, messageType uint16, valueSize uint32, position int64) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.items[sequence] = Item{sequence, partitionKey, messageType, valueSize, position}

	if this.lastKey < sequence {
		this.lastKey = sequence
	}

	this.empty = false
}

func (this *SeqDir) AddAll(items ...Item) {
	if len(items) == 0 {
		return
	}

	this.lock.Lock()
	defer this.lock.Unlock()

	for _, item := range items {
		this.items[item.Sequence] = item

		if this.lastKey < item.Sequence {
			this.lastKey = item.Sequence
		}
	}

	this.empty = false
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
		// TODO: we don't need to check ok for all, just inspect this.lastKey and break
		// if we exceeded it.
		item, ok := this.items[sequence]

		if ok {
			items = append(items, item)
		} else {
			break
		}
	}

	return items
}

// Gets the last key value in the directory.
func (this *SeqDir) GetLastKey() (key uint64, ok bool) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	return this.lastKey, !this.empty
}
