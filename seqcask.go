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

type Seqcask struct {
	activeFile         *os.File
	activeFilePosition int64
	batch              WriteBatch

	sequence uint64

	seqdir *SeqDir

	writeQueue chan *WriteBatch
}

func MustOpen(directory string, size int64) *Seqcask {
	if seqcask, err := Open(directory, size); err != nil {
		panic(err)
	} else {
		return seqcask
	}
}

func Open(directory string, size int64) (*Seqcask, error) {
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
		activeFile: file,
		seqdir:     NewSeqDir(),
		writeQueue: make(chan *WriteBatch, 16),
	}
	if err := cask.activeFile.Truncate(size); err != nil {
		return nil, err
	}
	go cask.writeLoop()

	return cask, nil
}

func (this *Seqcask) Put(value []byte) (err error) {
	batch := NewWriteBatch()
	batch.Put(value)

	// TODO: optimize for this use case as well
	return this.Write(batch)
}

type writeRequest struct {
	bytes []byte

	// represents the number of values stored in the bytes array
	valueCount int

	// the position in the file
	// where the first write happened
	position int64
	sequence uint64
	err      error

	done chan struct{}
}

func (this *Seqcask) writeLoop() {
	var writeStarted time.Time
	var written int

	for batch := range this.writeQueue {
		if glog.V(2) {
			writeStarted = time.Now()
		}

		// WriteAt doesn't use a atomic operation to synchronize position and
		// only uses a single syscall instead of two.
		written, batch.writeErr = this.activeFile.WriteAt(batch.Bytes(), this.activeFilePosition)

		if batch.writeErr != nil {
			if glog.V(1) {
				glog.Warningf("error writing %v bytes to active file %v at position %v: %v",
					batch.Len(), this.activeFile.Name(), this.activeFilePosition, batch.writeErr)
			}
		} else {
			// Set state as it was when starting this write
			batch.writePosition = this.activeFilePosition
			batch.writeSequence = this.sequence

			// Advance position because we succeeded.
			this.activeFilePosition += int64(written)
			this.sequence += uint64(batch.Len())

		}

		batch.writeDone <- struct{}{}
		if glog.V(2) {
			elapsed := time.Since(writeStarted)
			glog.Infof("written %v bytes in %v", written, elapsed)
		}
	}
}

func (this *Seqcask) Write(batch *WriteBatch) (err error) {
	this.writeQueue <- batch
	<-batch.writeDone

	if err = batch.writeErr; err != nil {
		return
	}

	// add all seqdir items to the seqdir
	this.seqdir.AddAll(batch.writeSequence, batch.getSeqdirItems()...)
	return
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

	valueData := buffer[4 : 4+valueSize]
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
