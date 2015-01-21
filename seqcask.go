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
)

var (
	ErrNotFound = errors.New("not found")
)

const (
	SIZE_CHECKSUM = 64 / 8
)

type Seqcask struct {
	buffer             *bytes.Buffer
	activeFile         *os.File
	activeFilePosition int64

	sequence uint64

	keydir *SeqDir
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

func NewKeydir() *SeqDir {
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

	return &Seqcask{
		activeFile: file,
		buffer:     new(bytes.Buffer),
		keydir:     NewKeydir(),
	}, nil
}

func (this *Seqcask) Put(value []byte) (seq uint64, err error) {
	seq = this.sequence

	vsz := uint16(len(value))
	vpos := this.activeFilePosition + int64(this.buffer.Len()) + 8 + 2
	//tstamp := time.Now().UnixNano()

	binary.Write(this.buffer, binary.LittleEndian, seq)
	binary.Write(this.buffer, binary.LittleEndian, vsz) // 2 bytes
	//binary.Write(this.buffer, binary.LittleEndian, tstamp)
	this.buffer.Write(value)

	//bufferSlice := this.buffer.Bytes()
	//dataToCrc := bufferSlice[len(bufferSlice)-length:]
	//checksum := xxhash.Checksum64(dataToCrc)
	//binary.Write(this.buffer, binary.LittleEndian, checksum)
	// TODO: re-enable crc
	binary.Write(this.buffer, binary.LittleEndian, 0)

	// TODO: set file id
	this.keydir.Add(seq, 1, vsz, vpos)

	this.sequence = seq + 1
	return
}

func (this *Seqcask) Get(seq uint64) ([]byte, error) {
	// entry, ok := this.keydir.Get(seq)
	// if !ok {
	// 	return nil, ErrNotFound
	// }
	//
	// buffer := make([]byte, 0, entry.ValueSize+SIZE_CHECKSUM)
	// if read, err := this.activeFile.ReadAt(buffer, entry.VPos); err != nil {
	// 	return nil, err
	// } else if read != int(entry.Vsz)+SIZE_CHECKSUM {
	// 	return nil, errors.New("read to short")
	// }
	//
	// valueData := buffer[:len(buffer)-SIZE_CHECKSUM]
	// checksumData := buffer[len(buffer)-SIZE_CHECKSUM:]
	// checksum := binary.LittleEndian.Uint64(checksumData)
	//
	// if xxhash.Checksum64(valueData) != checksum {
	// 	return nil, errors.New("checksum failed")
	// }
	//
	// return valueData, nil
	panic(errors.New("NOT IMPLEMENTED"))
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
