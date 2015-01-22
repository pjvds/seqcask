package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
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

	random := seqcask.NewRandomValueGenerator(*msgSize)
	timeout := time.After(*duration)
	defer random.Stop()

	batch := make([][]byte, 0, *batchSize)
	putted := 0

	startedAt := time.Now()
	for timedOut := false; !timedOut; {
		select {
		case value := <-random.Values:
			batch = append(batch, value)
			if len(batch) == *batchSize {
				if err := cask.PutBatch(batch...); err != nil {
					panic(err)
				}
				cask.Sync()

				putted += len(batch)
				batch = batch[0:0]
			}
		case <-timeout:
			timedOut = true
		}
	}

	elapsed := time.Since(startedAt)
	fmt.Printf("%v messages written in %v, %.0fmsg/s\n", putted, elapsed, float64(putted)/elapsed.Seconds())
}
