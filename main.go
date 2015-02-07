package main

import (
    "flag"
    "github.com/pjvds/seqcask/broker"
    "fmt"
    
    "os/signal"
    "os"
)

func main() {
    flag.Parse()


    b, err := broker.NewBroker()

    if err != nil {
        fmt.Printf("failed to initialize: %v", err.Error())
        return
    }

    stop := make(chan struct{})
    stopped := make(chan struct{})

    signals := make(chan os.Signal)
    signal.Notify(signals, os.Interrupt)

    go func() {
        defer close(stopped)

        if err := b.Run(stop); err != nil {
            fmt.Printf("broker failed: %v", err.Error())
        }
    }()

    select {
    case <-signals:
        fmt.Printf("stopping...")
        close(stop)
    case <-stopped:
        return
    }

    <-stopped
}
