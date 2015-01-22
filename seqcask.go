package seqcask

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/OneOfOne/xxhash/native"
)

var (
	ErrNotFound = errors.New("not found")
)

const (
	SIZE_CHECKSUM   = 64 / 8
	SIZE_SEQ        = 64 / 8
	SIZE_VALUE_SIZE = 16 / 8
)

type Seqcask struct {
	buffer             *bytes.Buffer
	activeFile         *os.File
	activeFilePosition int64

	sequence uint64

	seqdir *SeqDir
}

type Item struct {
	FileId    uint16
	ValueSize uint16
	Position  int64
}

type SeqDir struct {
	shards     []map[uint64]Item
	shardSize  int
	shardLocks []sync.RWMutex
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

func (this *SeqDir) Add(seq uint64, fid, vsz uint16, vpos int64) {
	shard, lock := this.getShard(seq)

	lock.Lock()
	shard[seq] = Item{fid, vsz, vpos}
	lock.Unlock()
}

func (this *SeqDir) Get(seq uint64) (*Item, bool) {
	shard, lock := this.getShard(seq)

	lock.RLock()
	item, ok := shard[seq]
	lock.RUnlock()

	return &item, ok
}

type header struct {
	tstamp uint64
	ksz    uint16
	vsz    uint16
}

func MustOpen(directory string) *Seqcask {
	if seqcask, err := Open(directory); err != nil {
		panic(err)
	} else {
		return seqcask
	}
}

func Open(directory string) (*Seqcask, error) {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	if len(files) != 0 {
		// TODO: support existing directories
		return nil, fmt.Errorf("directory not empty")
	}

	file, err := os.Create(path.Join(directory, "1.data"))
	if err != nil {
		return nil, err
	}

	cask := &Seqcask{
		activeFile: file,
		seqdir:     NewSeqDir(),
		buffer:     new(bytes.Buffer),
	}
	cask.buffer.Grow(5 * 1000 * 1024) // grow to 5MB
	return cask, nil
}

// func (this *Seqcask) Put(value []byte) (seq uint64, err error) {
// 	defer this.buffer.Reset(this.activeFile)
//
// 	seq = this.sequence
// 	valueSize := uint16(len(value))
// 	position := this.activeFilePosition
//
// 	binary.Write(this.buffer, binary.LittleEndian, seq)
// 	binary.Write(this.buffer, binary.LittleEndian, valueSize)
// 	this.buffer.Write(value)
//
// 	bufferSlice := this.buffer.Bytes()
// 	dataToCrc := bufferSlice[position-this.activeFilePosition:]
// 	checksum := xxhash.Checksum64(dataToCrc)
// 	binary.Write(this.buffer, binary.LittleEndian, checksum)
//
// 	// TODO: inspect written
// 	if _, err = this.buffer.WriteTo(this.activeFile); err != nil {
// 		return
// 	}
//
// 	// TODO: set file id
// 	this.seqdir.Add(seq, 1, valueSize, position)
// 	this.sequence = seq + 1
// 	return
// }

func (this *Seqcask) PutBatch(values ...[]byte) (err error) {
	this.buffer.Reset()

	transientSeq := this.sequence

	for _, value := range values {
		position := this.activeFilePosition + int64(this.buffer.Len())

		valueSize := uint16(len(value))
		binary.Write(this.buffer, binary.LittleEndian, transientSeq)
		binary.Write(this.buffer, binary.LittleEndian, valueSize)
		this.buffer.Write(value)

		bufferSlice := this.buffer.Bytes()
		dataToCrc := bufferSlice[position-this.activeFilePosition:]
		checksum := xxhash.Checksum64(dataToCrc)
		binary.Write(this.buffer, binary.LittleEndian, checksum)

		transientSeq++
	}

	if _, err = this.buffer.WriteTo(this.activeFile); err != nil {
		// TODO: unwrite written?
	}

	this.sequence = transientSeq
	// TODO: set seqdir items
	return
}

func (this *Seqcask) Get(seq uint64) ([]byte, error) {
	entry, ok := this.seqdir.Get(seq)
	if !ok {
		return nil, ErrNotFound
	}

	entryLength := SIZE_SEQ + SIZE_VALUE_SIZE + entry.ValueSize + SIZE_CHECKSUM
	buffer := make([]byte, entryLength, entryLength)
	if read, err := this.activeFile.ReadAt(buffer, entry.Position); err != nil {
		return nil, err
	} else if read != len(buffer) {
		return nil, errors.New("read to short")
	}

	checksumData := buffer[:len(buffer)-SIZE_CHECKSUM]
	checksum := binary.LittleEndian.Uint64(buffer[len(buffer)-SIZE_CHECKSUM:])
	if xxhash.Checksum64(checksumData) != checksum {
		return nil, errors.New("checksum failed")
	}

	valueStart := int(SIZE_SEQ + SIZE_VALUE_SIZE)
	valueData := buffer[valueStart : valueStart+int(entry.ValueSize)]
	return valueData, nil
}

func (this *Seqcask) Sync() error {
	written, err := this.buffer.WriteTo(this.activeFile)
	if err != nil {
		return err
	}

	if written > 0 {
		if err = this.activeFile.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (this *Seqcask) Close() error {
	if err := this.Sync(); err != nil {
		return err
	}
	return this.activeFile.Close()
}
