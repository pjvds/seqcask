package seqcask

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	"time"
	"github.com/golang/glog"

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

type BatchWriteResult struct {
	FileOffset int64
	Offset     uint64
	Error      error
}

type Seqcask struct {
	activeFile         *os.File
	activeFilePosition int64

	sequence uint64

	seqdir *SeqDir

	prepareQueue chan *Messages
	writerQueue  chan *WriteBatch
}

func (this *Seqcask) prepareLoop() {
	var result BatchWriteResult
	var msgCount int

	batch := NewWriteBatch()
	items := make([]Item, 0)

	for messages := range this.prepareQueue {
		msgCount = len(messages.messages)

		batch.Put(messages.messages...)

		this.writerQueue <- batch
		result = <-batch.done

		if result.Error != nil {
			messages.done <- result.Error
			continue
		}

		// make sure we have enought capacity in the item slice
		// we only care about capacity, not about the content so
		// recreating it is not a problem at all
		if cap(items) < msgCount {
			items = make([]Item, msgCount, msgCount)
		}

		// create seqdir items for every message
		for index, value := range messages.messages {
			items[index] = Item{
				FileId:    0,                  // TODO: set
				ValueSize: uint16(len(value)), // TODO: make uint32
				Position:  result.FileOffset + int64(batch.positions[index]),
			}
		}

		// add all seqdir items to the seqdir
		this.seqdir.AddAll(result.Offset, items[:msgCount]...)

		messages.done <- nil

		batch.Reset()
	}
}

func (this *Seqcask) writeLoop() {
	var err error
	var written int

	var writeStarted time.Time
	var writeFinished time.Time

	for batch := range this.writerQueue {
		if glog.V(2) {
			writeStarted = time.Now()
			glog.Infof("writer dequeue latency: %v", (writeStarted.Sub(writeFinished)))
		}

		// WriteAt doesn't use a atomic operation to synchronize position and
		// only uses a single syscall instead of two.
		written, err = this.activeFile.WriteAt(batch.Bytes(), this.activeFilePosition)

		if err != nil {
			batch.done <- BatchWriteResult{
				Error: err,
			}
		} else {
			this.activeFilePosition += int64(written)

			batch.done <- BatchWriteResult{
				Offset: this.sequence,
				Error:  nil,
			}
			this.sequence += uint64(batch.Len())
		}

		if glog.V(2) {
			elapsed := time.Since(writeStarted)
			glog.Infof("written %v bytes in %v", written, elapsed)
			writeFinished = time.Now()
		}
	}
}

type header struct {
	tstamp uint64
	ksz    uint16
	vsz    uint16
}

func MustOpen(directory string, size int64) *Seqcask {
	if seqcask, err := Open(directory, size); err != nil {
		panic(err)
	} else {
		return seqcask
	}
}

func Open(directory string, size int64) (*Seqcask, error) {
	const PREPARERS_COUNT = 6

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
		writerQueue:  make(chan *WriteBatch, PREPARERS_COUNT),
	}
	if err := cask.activeFile.Truncate(size); err != nil {
		return nil, err
	}

	for i := 0; i < PREPARERS_COUNT; i++ {
		go cask.prepareLoop()
	}
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

func (this *Seqcask) Put(value []byte) (err error) {
	// TODO: optimize for this use case as well
	return this.PutBatch(value)
}

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

	valueSize := int(entry.ValueSize)
	entryLength := 4 + valueSize + 8
	buffer := make([]byte, entryLength, entryLength)
	if read, err := this.activeFile.ReadAt(buffer, entry.Position); err != nil {
		log.Printf("error reading value from offset %v at file position %v to position %v, read %v bytes: %v", seq, entry.Position, entry.Position+int64(entryLength), read, err.Error())
		return nil, err
	} else if read != len(buffer) {
		return nil, errors.New("read to short")
	}

	valueData := buffer[4:4+valueSize]
	checksumData := buffer[4+valueSize:]

	// compare checksum of value to checksum from storage
	if xxhash.Checksum64(valueData) != binary.BigEndian.Uint64(checksumData) {
		return nil, errors.New("checksum failed")
	}

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
