package seqcask

import (
	"fmt"
	"os"

	"github.com/golang/glog"
)

type writer struct {
	file *os.File

	filePosition int64
	sequence     uint64
}

func (this *writer) Write(msgCount int, data []byte) (sequence uint64, position int64, err error) {
	if msgCount == 0 {
		return 0, 0, fmt.Errorf("can't write 0 messages")
	}

	var written int
	written, err = this.file.WriteAt(data, this.filePosition)

	if err != nil {
		if glog.V(1) {
			glog.Warningf("error writing %v bytes to active file %v at position %v: %v",
				msgCount, this.file.Name(), this.filePosition, err.Error())
		}
		return
	}

	// Set state as it was when starting this write
	sequence = this.sequence
	position = this.filePosition

	// Advance position because we succeeded.
	this.filePosition += int64(written)
	this.sequence += uint64(msgCount)
	return
}

func (this *writer) Sync() error {
	return this.file.Sync()
}
