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
	partition = flag.Int("partition", 1, "the partition to write to")

	workers = flag.Int("workers", 500, "the workers that will be sending")
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

			for n := 0; n < (2e6 / *workers); n++ {
				result := producer.Publish(*topic, (uint16(3)%uint16(*partition) + uint16(1)), message)
				if err := result.WaitForDone(1 * time.Second); err != nil {
					fmt.Printf("publish failed: %v\n", err.Error())
				}
				//fmt.Printf("%v\n", n)
			}
		}()
	}

	work.Wait()
	elapsed := time.Since(startedAt)
	msgsPerSecond := float64(2e6) / elapsed.Seconds()
	mbPerSecond := (msgsPerSecond * 200.0) / 1000.0 / 1000.0
	fmt.Printf("%v in %v, %v msg/s\n aka %v mb/s", 2e6, elapsed, float64(2e6)/elapsed.Seconds(), mbPerSecond)
}
