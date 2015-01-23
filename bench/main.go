package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pjvds/seqcask"
)

var (
	msgSize   = flag.Int("msgsize", 200, "the size of the messages in byte")
	duration  = flag.Duration("duration", 10*time.Second, "the time to run the benchmark")
	batchSize = flag.Int("batchsize", 64, "the number of message to batch in a single put")
)

func main() {
	flag.Parse()
	directory, _ := ioutil.TempDir("", "seqcask_bench_")
	cask := seqcask.MustOpen(directory)
	defer cask.Close()
	defer os.RemoveAll(directory)

	//random := seqcask.NewRandomValueGenerator(*msgSize)
	value := []byte("mggzshawiqilnhkrlehaeejlcgwrhqdhhghvqgeuedwpzeyazhwudeoxahsywvvlxwsikadidxrsgpwndfimkkalhybcsxpmoayultaycuigbjpcjvqmcyeaxkcgvklfykpjykbfdwjbebfwlayvgdfiuceembvkijppatzrjibibdjjphwzyvlyxjslpkxvuwqrscbr")
	//defer random.Stop()

	batch := make([][]byte, *batchSize, *batchSize)
	for i, _ := range batch {
		batch[i] = value
	}
	putted := new(int64)

	var work sync.WaitGroup
	startedAt := time.Now()

	var done bool

	work.Add(1)
	go func() {
		for !done {
			if err := cask.PutBatch(batch...); err != nil {
				panic(err)
			}
			atomic.AddInt64(putted, int64(len(batch)))
		}
		work.Done()
	}()

	work.Add(1)
	go func() {
		for !done {
			if err := cask.PutBatch(batch...); err != nil {
				panic(err)
			}
			atomic.AddInt64(putted, int64(len(batch)))
		}
		work.Done()
	}()

	work.Add(1)
	go func() {
		for !done {
			if err := cask.PutBatch(batch...); err != nil {
				panic(err)
			}
			atomic.AddInt64(putted, int64(len(batch)))
		}
		work.Done()
	}()

	<-time.After(*duration)
	done = true

	work.Wait()
	cask.Sync()

	elapsed := time.Since(startedAt)
	totalMb := float64(*putted*int64(*msgSize)) / float64(1000*1024)
	fmt.Printf("%v messages written in %v, %.0fmsg/s, %0.3fmb/s\n", *putted, elapsed, float64(*putted)/elapsed.Seconds(), totalMb/elapsed.Seconds())

	dataFilename := filepath.Join(directory, "1.data")
	stats, err := os.Stat(dataFilename)
	if err != nil {
		fmt.Printf("failed to stat %v: %v", dataFilename, err.Error())
	} else {
		fmt.Printf("%v: %vmb", dataFilename, stats.Size()/int64(1048576))

	}
}
