package seqcask_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pjvds/seqcask"
)

func (this *RandomValueGenerator) Stop() {
	close(this.stop)
}

func BenchmarkPut(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustOpen(directory)
	random := seqcask.NewRandomValueGenerator(200)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cask.Put(<-random.Values); err != nil {
			b.Fatalf("failed to put: %v", err.Error())
		}
	}
	cask.Sync()
}

func BenchmarkPutBatch(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustOpen(directory)
	random := NewRandomValueGenerator(200)

	values := make([][]byte, 1000*1000, 1000*1000)
	for index := range values {
		values[index] = <-random.Values
	}

	random.Stop()

	b.SetBytes(int64(len(values) * 200))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cask.PutBatch(values...); err != nil {
			b.Fatalf("failed to put: %v", err.Error())
		}
	}
	cask.Sync()
}

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
