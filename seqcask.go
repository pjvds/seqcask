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
	//"github.com/ncw/directio"
)

var (
	ErrNotFound = errors.New("not found")
)

const (
	SIZE_CHECKSUM   = 64 / 8
	SIZE_SEQ        = 64 / 8
	SIZE_VALUE_SIZE = 16 / 8
)

type Messages struct {
	messages [][]byte

	done chan error
}

type Batch struct {
	buffer            *bytes.Buffer
	sequencePositions []int

	done chan error
}

func (this *Batch) SetSequences(sequence uint64) {
	bytes := this.buffer.Bytes()
	for _, position := range this.sequencePositions {
		binary.LittleEndian.PutUint64(bytes[position:], sequence)
		sequence++
	}
}

func (this *Batch) Len() int {
	return len(this.sequencePositions)
}

type Seqcask struct {
	activeFile         *os.File
	activeFilePosition int64

	sequence uint64

	seqdir *SeqDir

	prepareQueue chan *Messages
	writerQueue  chan *Batch
}

func (this *Seqcask) prepareLoop() {
	batch := Batch{
		buffer: new(bytes.Buffer),
		done:   make(chan error, 1),
	}
	for messages := range this.prepareQueue {

		transientSeq := 0 // TODO: set, or re-align sequence?
		transientPos := 0

		for _, value := range messages.messages {
			// TODO: position := this.activeFilePosition + int64(this.buffer.Len())
			transientPos = batch.buffer.Len()

			valueSize := uint16(len(value))
			batch.sequencePositions = append(batch.sequencePositions, batch.buffer.Len())

			binary.Write(batch.buffer, binary.LittleEndian, transientSeq) // can't we just skip those bytes?
			binary.Write(batch.buffer, binary.LittleEndian, valueSize)
			batch.buffer.Write(value)

			bufferSlice := batch.buffer.Bytes()
			dataToCrc := bufferSlice[transientPos:]
			checksum := xxhash.Checksum64(dataToCrc)
			binary.Write(batch.buffer, binary.LittleEndian, checksum)

			// TODO: transientSeq++
		}

		this.writerQueue <- &batch
		err := <-batch.done
		messages.done <- err

		batch.buffer.Reset()
		batch.sequencePositions = batch.sequencePositions[0:0]
	}
}

func (this *Seqcask) writeLoop() {
	var err error
	for batch := range this.writerQueue {
		batch.SetSequences(this.sequence)

		//_, err = batch.buffer.WriteTo(this.activeFile);
		if _, err = this.activeFile.Write(batch.buffer.Bytes()); err != nil {
			batch.done <- err
		} else {
			this.sequence += uint64(batch.Len())
			batch.done <- nil
		}
	}
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

	//file, err := directio.OpenFile(path.Join(directory, "1.data"), os.O_CREATE | os.O_WRONLY, 0666)
	file, err := os.Create(path.Join(directory, "1.data"))
	if err != nil {
		return nil, err
	}

	cask := &Seqcask{
		activeFile:   file,
		seqdir:       NewSeqDir(),
		prepareQueue: make(chan *Messages, 256),
		writerQueue:  make(chan *Batch),
	}

	go cask.prepareLoop()
	go cask.prepareLoop()
	go cask.prepareLoop()
	go cask.writeLoop()
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
	messages := &Messages{
		messages: values,
		done:     make(chan error, 1),
	}
	this.prepareQueue <- messages
	// TODO: set seqdir items
	return <-messages.done
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
	// written, err := this.buffer.WriteTo(this.activeFile)
	// if err != nil {
	// 	return err
	// }
	//
	//if written > 0 {
	if err := this.activeFile.Sync(); err != nil {
		return err
	}
	//}
	return nil
}

func (this *Seqcask) Close() error {
	if err := this.Sync(); err != nil {
		return err
	}
	return this.activeFile.Close()
}
