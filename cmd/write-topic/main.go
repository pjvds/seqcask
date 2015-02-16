package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/davecheney/profile"
	"github.com/golang/glog"
	"github.com/pjvds/randombytes"
	"github.com/pjvds/seqcask/producer"
)

var (
	address   = flag.String("address", "tcp://127.0.0.1:40899", "the address of the broker")
	topic     = flag.String("topic", "test", "the topic to write to")
	partition = flag.Int("partition", 3, "the partition to write to")
	count     = flag.Int("count", 5*1000*1000, "number of messages to send")

	workers = flag.Int("workers", 10000, "the workers that will be sending")
)

func main() {
	defer profile.Start(profile.CPUProfile).Stop()

	producer, err := producer.NewProducer(*address)
	if err != nil {
		glog.Fatalf("failed: %v", err.Error())
	}

	var work sync.WaitGroup
	message := randombytes.Make(200)
	startedAt := time.Now()

	for i := 0; i < *workers; i++ {
		work.Add(1)
		go func() {
			defer work.Done()

			perWorkerCount := (*count / *workers)

			for n := 0; n < perWorkerCount; n++ {
				part := uint16((n % 3) + 1)

				result := producer.Publish(*topic, part, message)
				if err := result.WaitForDone(); err != nil {
					fmt.Printf("publish failed: %v\n", err.Error())
				}
			}
		}()
	}

	work.Wait()
	elapsed := time.Since(startedAt)
	msgsPerSecond := float64(2e6) / elapsed.Seconds()
	mbPerSecond := (msgsPerSecond * 200.0) / 1000.0 / 1000.0
	fmt.Printf("%v in %v, %f msg/s\n aka %f mb/s", *count, elapsed, float64(*count)/elapsed.Seconds(), mbPerSecond)
}
