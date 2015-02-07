package storage

import (
	"encoding/binary"
	"errors"
	"log"
	"os"

	"github.com/OneOfOne/xxhash/native"
	"github.com/golang/glog"
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
	activeFile *os.File
	batch      WriteBatch

	seqdir *SeqDir

	writer chan *writer
}

type KeyValuePair struct {
	Key   uint64
	Value []byte
}

func MustCreate(filename string, size int64) *Seqcask {
	if seqcask, err := Create(filename, size); err != nil {
		panic(err)
	} else {
		return seqcask
	}
}

func Create(filename string, size int64) (*Seqcask, error) {
	//file, err := directio.OpenFile(path.Join(directory, "1.data"), os.O_CREATE | os.O_WRONLY, 0666)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
	if err != nil {
		return nil, err
	}

	cask := &Seqcask{
		activeFile: file,
		seqdir:     NewSeqDir(),
		writer:     make(chan *writer, 1),
	}
	cask.writer <- &writer{
		file: file,
	}
	if err := cask.activeFile.Truncate(size); err != nil {
		return nil, err
	}

	return cask, nil
}

// Borrow the writer to do a file write. It should be
// returned by calling returnWriter as soon as possible.
//
// This methods blocks until the writer is available. There
// is not garrantee in order, the writer is handed out in
// an non deterministic order. So it might be that a goroutine
// requested it first but another routine that requested it later
// will actually get it.
func (this *Seqcask) borrowWriter() *writer {
	writer := <-this.writer

	if glog.V(2) {
		glog.Info("borrowing writer at %v", writer.sequence)
	}

	return writer
}

// Returns an writer after it has been borrowed. This should
// be called as soon as possible to make sure others can use
// the writer. This means you probably want to return it even
// before you check for errors or inspecting anything else.
func (this *Seqcask) returnWriter(writer *writer) {
	if glog.V(2) {
		glog.Info("returned writer at %v", writer.sequence)
	}

	this.writer <- writer
}

// Writes the content of the given write batch to the database.
// The write batch itself is left untouched and needs to be
// resetted after this method succeeded if it needs to be used
// again.
// The reason we do not reset the write batch from this method
// is to allow writing the same batch to multiple databases if
// needed.
func (this *Seqcask) Write(batch *WriteBatch) (err error) {
	var sequenceStart uint64
	var positionStart int64
	writer := this.borrowWriter()

	sequenceStart, positionStart, err = writer.Write(batch.Len(), batch.Bytes())
	this.returnWriter(writer)

	if err != nil {
		return
	}

	this.seqdir.AddAllOffset(sequenceStart, positionStart, batch.itemBuffer...)
	return
}

func (this *Seqcask) readValue(item Item) ([]byte, error) {
	valueSize := int(item.ValueSize)
	itemLength := 4 + valueSize + 8
	buffer := make([]byte, itemLength, itemLength)
	if read, err := this.activeFile.ReadAt(buffer, item.Position); err != nil {
		// TODO: set sequence/offset... prob we should just get it from item
		log.Printf("error reading value from offset %v at file position %v to position %v, read %v bytes: %v", 0, item.Position, item.Position+int64(itemLength), read, err.Error())
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

func (this *Seqcask) Get(key uint64) (KeyValuePair, error) {
	// TODO: support GET FROM
	item, ok := this.seqdir.Get(key)
	if !ok {
		return KeyValuePair{}, ErrNotFound
	}

	if value, err := this.readValue(item); err != nil {
		return KeyValuePair{}, err
	} else {
		return KeyValuePair{
			Key:   key,
			Value: value,
		}, nil
	}
}

func (this *Seqcask) GetLastKey() (uint64, bool) {
	return this.seqdir.GetLastKey()
}

// GetAll returns all available values from a key to the maximum of the given length.
// If there are no values available in that range, an empty slice is returned.
func (this *Seqcask) GetAll(key uint64, length int) ([]KeyValuePair, error) {
	items := this.seqdir.GetAll(key, length)
	itemCount := len(items)

	values := make([]KeyValuePair, itemCount, itemCount)

	// if we have no items in that range, just return an empty slice
	if itemCount == 0 {
		return values, nil
	}

	// we have a single item, read the value and return it in a slice
	if len(items) == 1 {
		if value, err := this.readValue(items[0]); err != nil {
			return nil, err
		} else {
			values[0] = KeyValuePair{
				Key:   key, // TODO: get key from item
				Value: value,
			}
			return values, nil
		}
	}

	// TODO: handle corrupt items by advancing to next one
	totalSize := 0
	overhead := (32 / 8) + (64 / 8)
	for _, item := range items {
		totalSize += int(item.ValueSize) + overhead
	}

	buffer := make([]byte, totalSize, totalSize)
	if _, err := this.activeFile.ReadAt(buffer, items[0].Position); err != nil {
		return nil, err
	}

	position := (32 / 8)

	for index, item := range items {
		values[index] = KeyValuePair{
			Key:   key + uint64(index), // TODO: get key from item
			Value: buffer[position : position+int(item.ValueSize)],
		}
		position += int(item.ValueSize) + overhead
	}

	return values, nil
}

func (this *Seqcask) Sync() error {
	writer := this.borrowWriter()
	defer this.returnWriter(writer)

	return writer.Sync()
}

func (this *Seqcask) Close() error {
	if err := this.Sync(); err != nil {
		return err
	}
	return this.activeFile.Close()
}
