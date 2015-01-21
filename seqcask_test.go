package seqcask_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pjvds/seqcask"
)

func TestOpen(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	b, err := seqcask.Open(directory)
	if err != nil {
		t.Fatalf("failed to open bitcast at directory %v: %v", directory, err.Error())
	}
	defer b.Close()
}

func TestPut(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	b, err := seqcask.Open(directory)
	if err != nil {
		t.Fatalf("failed to open bitcast at directory %v: %v", directory, err.Error())
	}
	defer b.Close()

	putValues := [][]byte{
		[]byte("pieter joost van de sande"),
		[]byte("tomas roos"),
	}

	for index, value := range putValues {
		seq, err := b.Put(value)
		if err != nil {
			t.Fatalf("failed to put: %v", err.Error())
		}
		if seq != uint64(index) {
			t.Fatalf("unexpected seq: got %v, expected %v", seq, index)
		}
	}
}

func TestPutGetRoundtrup(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	b, err := seqcask.Open(directory)
	if err != nil {
		t.Fatalf("failed to open bitcast at directory %v: %v", directory, err.Error())
	}
	defer b.Close()

	putValue := []byte("hello world")

	if seq, err := b.Put(putValue); err != nil {
		t.Fatalf("failed to put: %v", err.Error())
	} else {
		if err := b.Sync(); err != nil {
			t.Fatalf("failed to sync: %v")
		}

		if getValue, err := b.Get(seq); err != nil {
			t.Fatalf("failed to get: %v", err.Error())
		} else {
			if !bytes.Equal(putValue, getValue) {
				t.Fatalf("put and get value differ: %v vs %v, %v vs %v", string(putValue), string(getValue), putValue, getValue)
			}
		}
	}
}
