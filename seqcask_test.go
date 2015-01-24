package seqcask_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pjvds/seqcask"
)

func BenchmarkPut(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustOpen(directory, 0)
	random := seqcask.NewRandomValueGenerator(200)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cask.Put(<-random.Values); err != nil {
			b.Fatalf("failed to put: %v", err.Error())
		}
	}
	cask.Sync()
}

func BenchmarkPutDirect(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustOpen(directory, 0)
	random := seqcask.NewRandomValueGenerator(200)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := <-random.Values
		if err := cask.PutBatchDirect(value); err != nil {
			b.Fatalf("failed to put: %v", err.Error())
		}
	}
	cask.Sync()
}

func BenchmarkPutBatch(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustOpen(directory, 5000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cask.PutBatch([]byte("pieter joost van de sande")); err != nil {
			b.Fatalf("failed to put: %v", err.Error())
		}
	}
	cask.Sync()
}

func TestOpen(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	b, err := seqcask.Open(directory, 0)
	if err != nil {
		t.Fatalf("failed to open bitcast at directory %v: %v", directory, err.Error())
	}
	defer b.Close()
}

func TestPut(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	b, err := seqcask.Open(directory, 0)
	if err != nil {
		t.Fatalf("failed to open bitcast at directory %v: %v", directory, err.Error())
	}
	defer b.Close()

	putValues := [][]byte{
		[]byte("pieter joost van de sande"),
		[]byte("tomas roos"),
	}

	for _, value := range putValues {
		if err := b.Put(value); err != nil {
			t.Fatalf("failed to put: %v", err.Error())
		}
	}
}

func TestPutDirect(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	b, err := seqcask.Open(directory, 0)
	if err != nil {
		t.Fatalf("failed to open bitcast at directory %v: %v", directory, err.Error())
	}
	defer b.Close()

	putValues := [][]byte{
		[]byte("pieter joost van de sande"),
		[]byte("tomas roos"),
	}

	b.PutBatchDirect(putValues...)
}

func TestPutGetRoundtrup(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	b, err := seqcask.Open(directory, 0)
	if err != nil {
		t.Fatalf("failed to open bitcast at directory %v: %v", directory, err.Error())
	}
	defer b.Close()

	putValue := []byte("hello world")

	if err := b.Put(putValue); err != nil {
		t.Fatalf("failed to put: %v", err.Error())
	} else {
		if err := b.Sync(); err != nil {
			t.Fatalf("failed to sync: %v")
		}

		if getValue, err := b.Get(0); err != nil {
			t.Fatalf("failed to get: %v", err.Error())
		} else {
			if !bytes.Equal(putValue, getValue) {
				t.Fatalf("put and get value differ: %v vs %v, %v vs %v", string(putValue), string(getValue), putValue, getValue)
			}
		}
	}
}
