package bitcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/OneOfOne/xxhash"
)

var (
	byteOrder = binary.BigEndian
)

type Bitcask struct {
	buffer     *bytes.Buffer
	activeFile *os.File
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
	}, nil
}

func (this *Bitcask) Put(key []byte, value []byte) (err error) {
	ksz := uint16(len(key))
	vsz := uint16(len(value))

	binary.Write(this.buffer, binary.LittleEndian, ksz) // 2 bytes
	binary.Write(this.buffer, binary.LittleEndian, vsz) // 2 bytes
	this.buffer.Write(key)
	this.buffer.Write(value)

	length := 2 + 2 + len(key) + len(value)
	bufferSlice := this.buffer.Bytes()
	dataToCrc := bufferSlice[len(bufferSlice)-length:]
	checksum := xxhash.Checksum64(dataToCrc)
	binary.Write(this.buffer, binary.LittleEndian, checksum)
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
