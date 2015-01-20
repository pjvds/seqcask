package bitcask_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/pjvds/bitcask"
)

func TestOpen(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	b, err := bitcask.Open(directory)
	if err != nil {
		t.Fatalf("failed to open bitcast at directory %v: %v", directory, err.Error())
	}
	defer b.Close()
}
