package storage_test

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/pjvds/seqcask"
	"github.com/stretchr/testify/assert"
)

func TestMultipleBatchRoundtrip(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	byteOrder := binary.LittleEndian

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	batch := seqcask.NewWriteBatch()
	batchSize := uint32(256)

	buffer := make([]byte, 32/8, 32/8)

	for n := uint32(1); n <= batchSize*5; n++ {
		byteOrder.PutUint32(buffer, n)

		if n%batchSize == 0 || n == 5*batchSize {
			cask.Write(batch)
			batch.Reset()
		}
	}

	keyValuesPairs, err := cask.GetAll(0, int(5*batchSize))
	assert.Nil(t, err)

	for index, pair := range keyValuesPairs {
		assert.Equal(t, index, pair.Key)
		valueAsUint32 := byteOrder.Uint32(pair.Value)

		assert.Equal(t, uint32(index+1), valueAsUint32)
	}
}

func TestPutBatchGetAll(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	batch := seqcask.NewWriteBatch()

	putMessages := make([][]byte, 50, 50)
	for index := range putMessages {
		putMessages[index] = RandomValue(200)
	}

	batch.Put(putMessages...)

	err := cask.Write(batch)
	assert.Nil(t, err)

	values, err := cask.GetAll(uint64(0), len(putMessages))

	assert.Equal(t, len(values), len(putMessages))

	for index, value := range values {
		assert.True(t, bytes.Equal(value.Value, putMessages[index]))
	}
}

func BenchmarkWrite1mb200bValuesAsync(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	defer cask.Close()

	batches := make([]*seqcask.WriteBatch, 50, 50)
	for batchIndex := range batches {
		// 5000 * 200 bytes values = 1 megabyte
		messages := make([][]byte, 5000, 5000)

		for index := range messages {
			messages[index] = RandomValue(200)
		}

		batch := seqcask.NewWriteBatch()
		batch.Put(messages...)

		batches[batchIndex] = batch
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.SetBytes(5000 * 200)
		batch := batches[i%50]

		if err := cask.Write(batch); err != nil {
			b.Fatalf("failed to write batch: %v", err.Error())
		}
	}
	b.StopTimer()
}

func BenchmarkWrite1mb200bValuesSync(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	defer cask.Close()

	batches := make([]*seqcask.WriteBatch, 50, 50)
	for batchIndex := range batches {
		// 5000 * 200 bytes values = 1 megabyte
		messages := make([][]byte, 5000, 5000)

		for index := range messages {
			messages[index] = RandomValue(200)
		}

		batch := seqcask.NewWriteBatch()
		batch.Put(messages...)

		batches[batchIndex] = batch
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.SetBytes(5000 * 200)
		batch := batches[i%50]

		if err := cask.Write(batch); err != nil {
			b.Fatalf("failed to write batch: %v", err.Error())
		}
		if err := cask.Sync(); err != nil {
			b.Fatalf("failed to sync seqcask db: %v", err.Error())
		}
	}
	b.StopTimer()
}

func BenchmarkGetRange1mb200bValues(b *testing.B) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	defer cask.Close()

	batch := seqcask.NewWriteBatch()

	for i := 0; i < 5000; i++ {
		value := RandomValue(200)
		batch.Put(value)
	}

	for i := 0; i < b.N; i++ {
		if err := cask.Write(batch); err != nil {
			b.Fatalf("failed to write: %v", err.Error())
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.SetBytes(5000 * 200)

		from := uint64(i * 5000)

		if items, err := cask.GetAll(from, 5000); err != nil {
			b.Fatalf("failed to read range: %v", err.Error())
		} else if len(items) != 5000 {
			lastKey, _ := cask.GetLastKey()
			b.Fatalf("too short read from sequence %v at iteration %v: %v, last key in db %v", from, i, len(items), lastKey)
		}
	}
	b.StopTimer()
}

func TestCreate(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 0)
	defer cask.Close()
}

/*func TestWrite(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	defer cask.Close()

	messages := []seqcask.Message{
		seqcask.NewMessage(0, 0, []byte("pieter joost van de sande")),
		seqcask.NewMessage(0, 0, []byte("tomas roos")),
	}

	batch := seqcask.NewWriteBatch()
	batch.Put(messages)

	if err := cask.Write(batch); err != nil {
		t.Fatalf("failed to write: %v", err.Error())
	}
}*/

func TestPutGetRoundtrip(t *testing.T) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	defer os.RemoveAll(directory)

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 5000)
	defer cask.Close()

	messages := [][]byte{
		[]byte("pieter joost van de sande"),
	}

	batch := seqcask.NewWriteBatch()
	batch.Put(messages...)

	if err := cask.Write(batch); err != nil {
		t.Fatalf("failed to write: %v", err.Error())
	} else {
		if err := cask.Sync(); err != nil {
			t.Fatalf("failed to sync: %v")
		}

		if getValue, err := cask.Get(0); err != nil {
			t.Fatalf("failed to get: %v", err.Error())
		} else {
			putValue := messages[0]

			if !bytes.Equal(putValue, getValue.Value) {
				t.Fatalf("put and get value differ: %v vs %v, %v vs %v", string(putValue), string(getValue.Value), putValue, getValue.Value)
			}
		}
	}
}
