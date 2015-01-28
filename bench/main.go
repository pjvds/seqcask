package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"

	"github.com/pjvds/seqcask"
)

var (
	msgSize   = flag.Int("msgsize", 200, "the size of the messages in byte")
	duration  = flag.Duration("duration", 10*time.Second, "the time to run the benchmark")
	batchSize = flag.Int("batchsize", 64, "the number of message to batch in a single put")
	dir       = flag.String("dir", "", "the directory to store the data files")
)

func main() {
	flag.Parse()
	glog.Info("starting...")

	directory, _ := ioutil.TempDir("", "seqcask_bench_")
	if len(*dir) > 0 {
		directory = *dir
	}

	cask := seqcask.MustCreate(filepath.Join(directory, "db.data"), 30000000000)
	defer cask.Close()
	defer os.RemoveAll(directory)

	value := make([]byte, *msgSize, *msgSize)
	for index := range value {
		value[index] = byte(rand.Intn(255))
	}

	putted := new(int64)

	var work sync.WaitGroup
	startedAt := time.Now()

	var done bool

	for i := 0; i < 16; i++ {
		work.Add(1)
		go func() {
			batch := seqcask.NewWriteBatch()
			for index := 0; index < *batchSize; index++ {
				batch.Put(value)
			}
			for !done {
				if err := cask.Write(batch); err != nil {
					panic(err)
				}
				atomic.AddInt64(putted, int64(batch.Len()))
			}
			work.Done()
		}()
	}

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
