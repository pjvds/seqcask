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

type Seqcask struct {
	activeFile         *os.File
	activeFilePosition int64
	batch              WriteBatch

	sequence uint64

	seqdir *SeqDir

	writer chan writer
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
		return nil, fmt.Errorf("directory %v not empty: contains %v files", directory, len(files))
	}

	//file, err := directio.OpenFile(path.Join(directory, "1.data"), os.O_CREATE | os.O_WRONLY, 0666)
	file, err := os.Create(path.Join(directory, "1.data"))
	if err != nil {
		return nil, err
	}

	cask := &Seqcask{
		activeFile: file,
		seqdir:     NewSeqDir(),
		writer:     make(chan writer, 1),
	}
	cask.writer <- writer{
		file: file,
	}
	if err := cask.activeFile.Truncate(size); err != nil {
		return nil, err
	}

	return cask, nil
}

func (this *Seqcask) Put(value []byte) (err error) {
	batch := NewWriteBatch()
	batch.Put(value)

	// TODO: optimize for this use case as well
	return this.Write(batch)
}

// Borrow the writer to do a file write. It should be
// returned by calling returnWriter as soon as possible.
//
// This methods blocks until the writer is available. There
// is not garrantee in order, the writer is handed out in
// an non deterministic order. So it might be that a goroutine
// requested it first but another routine that requested it later
// will actually get it.
func (this *Seqcask) borrowWriter() writer {
	return <-this.writer
}

// Returns an writer after it has been borrowed. This should
// be called as soon as possible to make sure others can use
// the writer. This means you probably want to return it even
// before you checked for errors or anything.
func (this *Seqcask) returnWriter(writer writer) {
	this.writer <- writer
}

func (this *Seqcask) Write(batch *WriteBatch) (err error) {
	var sequenceStart uint64
	var positionStart int64
	writer := this.borrowWriter()

	sequenceStart, positionStart, err = writer.Write(batch.Len(), batch.Bytes())
	this.returnWriter(writer)

	if err != nil {
		return
	}

	// add all seqdir items to the seqdir
	this.seqdir.AddAll(sequenceStart, batch.getSeqdirItems(positionStart)...)
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
