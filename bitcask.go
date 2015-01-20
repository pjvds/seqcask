package bitcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/OneOfOne/xxhash"
)

var (
	byteOrder = binary.BigEndian
)

type Bitcask struct {
	buffer     *bytes.Buffer
	activeFile *os.File

	keydir *Keydir
}

type Item struct {
	Fid    uint16
	Vsz    uint16
	Vpos   uint32
	Tstamp uint64
}

type Keydir struct {
	shards     []map[string]Item
	shardLocks []sync.RWMutex
}

func NewKeydir() *Keydir {
	shards := make([]map[string]Item, 256)
	for i := 0; i < len(shards); i++ {
		shards[i] = make(map[string]Item, 1024)
	}

	return &Keydir{
		shards:     shards,
		shardLocks: make([]sync.RWMutex, 256, 256),
	}
}

func (this *Keydir) Add(key []byte, fid, vsz uint16, vpos uint32, tstamp uint64) {
	fb := key[0]
	shard := this.shards[fb]
	lock := this.shardLocks[fb]

	lock.Lock()
	shard[string(key)] = Item{fid, vsz, vpos, tstamp}
	lock.Unlock()
}

func (this *Keydir) Get(key []byte) (*Item, bool) {
	fb := key[0]
	shard := this.shards[fb]
	lock := this.shardLocks[fb]

	lock.RLock()
	item, ok := shard[string(key)]
	lock.RUnlock()

	return &item, ok
}

func (this *Keydir) Remove(key string) {
	fb := key[0]
	shard := this.shards[fb]
	lock := this.shardLocks[fb]

	lock.Lock()
	delete(shard, string(key))
	lock.Unlock()
}

type header struct {
	tstamp uint64
	ksz    uint16
	vsz    uint16
}

func Open(directory string) (*Bitcask, error) {
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

	return &Bitcask{
		activeFile: file,
		buffer:     new(bytes.Buffer),
		keydir:     NewKeydir(),
	}, nil
}

func (this *Bitcask) Put(key []byte, value []byte) (err error) {
	ksz := uint16(len(key))
	vsz := uint16(len(value))
	//tstamp := time.Now().UnixNano()

	binary.Write(this.buffer, binary.LittleEndian, ksz) // 2 bytes
	binary.Write(this.buffer, binary.LittleEndian, vsz) // 2 bytes
	//binary.Write(this.buffer, binary.LittleEndian, tstamp)
	this.buffer.Write(key)
	this.buffer.Write(value)

	length := 4 + len(key) + len(value)
	bufferSlice := this.buffer.Bytes()
	dataToCrc := bufferSlice[len(bufferSlice)-length:]
	checksum := xxhash.Checksum64(dataToCrc)
	binary.Write(this.buffer, binary.LittleEndian, checksum)

	this.keydir.Add(key, ksz, vsz, 0, 0)
	return
}

func (this *Bitcask) Sync() error {
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

func (this *Bitcask) Close() error {
	if err := this.Sync(); err != nil {
		return err
	}
	return this.activeFile.Close()
}
