package seqcask

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

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
	batch := NewWriteBatch()

	for messages := range this.prepareQueue {
		batch.Put(messages.messages...)

		this.writerQueue <- batch
		result = <-batch.done

		if result.Error != nil {
			messages.done <- result.Error
			continue
		}

		items := make([]Item, len(messages.messages), len(messages.messages))
		for index, value := range messages.messages {
			items[index] = Item{
				FileId:    0,                  // TODO: set
				ValueSize: uint16(len(value)), // TODO: make uint32
				Position:  result.FileOffset + int64(batch.positions[index]),
			}
		}

		this.seqdir.AddAll(result.Offset, items...)
		messages.done <- nil

		batch.Reset()
	}
}

func (this *Seqcask) writeLoop() {
	var err error

	for batch := range this.writerQueue {
		//_, err = batch.buffer.WriteTo(this.activeFile);
		if _, err = batch.WriteTo(this.activeFile); err != nil {
			batch.done <- BatchWriteResult{
				Error: err,
			}
		} else {
			batch.done <- BatchWriteResult{
				Offset: this.sequence,
				Error:  nil,
			}
			this.sequence += uint64(batch.Len())
		}
	}
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
		writerQueue:  make(chan *WriteBatch),
	}

	go cask.prepareLoop()
	go cask.prepareLoop()
	go cask.prepareLoop()
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

	entryLength := 8 + 4 + entry.ValueSize + 8
	buffer := make([]byte, entryLength, entryLength)
	if read, err := this.activeFile.ReadAt(buffer, entry.Position); err != nil {
		log.Printf("error reading value from offset %v at file position %v to position %v, read %v bytes: %v", seq, entry.Position, entry.Position+int64(entryLength), read, err.Error())
		return nil, err
	} else if read != len(buffer) {
		return nil, errors.New("read to short")
	}

	checksumData := buffer[:len(buffer)-SIZE_CHECKSUM]
	checksum := binary.LittleEndian.Uint64(buffer[len(buffer)-SIZE_CHECKSUM:])
	if xxhash.Checksum64(checksumData) != checksum {
		return nil, errors.New("checksum failed")
	}

	valueStart := int(SIZE_SEQ + 4)
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
