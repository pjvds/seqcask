package cluster

import (
	"sync"

	"github.com/hashicorp/consul/api"
)

type KeyAndMonitor struct {
	*KeyMonitor

	InitialValue *api.KVPair
}

func GetKeyAndMonitor(consul *api.Client, key string) (*KeyAndMonitor, error) {
	kv := consul.KV()

	initial, meta, err := kv.Get(key, nil)
	if err != nil {
		return nil, err
	}

	return &KeyAndMonitor{
		InitialValue: initial,
		KeyMonitor:   NewKeyMonitor(consul, key, meta.LastIndex),
	}, nil
}

// Watch a key in consul
type KeyMonitor struct {
	consul    *api.Client
	Key       string
	waitIndex uint64

	Changed  chan *api.KVPair
	stop     chan bool
	stopOnce sync.Once
}

func NewKeyMonitor(consul *api.Client, key string, waitIndex uint64) (monitor *KeyMonitor) {
	monitor = &KeyMonitor{
		consul:    consul,
		Key:       key,
		waitIndex: waitIndex,

		Changed: make(chan *api.KVPair),
		stop:    make(chan bool),
	}
	go monitor.do()
	return
}

func (this *KeyMonitor) do() {
	defer func() {
		close(this.Changed)
	}()

	kv := this.consul.KV()
	changes := make(chan *api.KVPair)

	go func() {
		defer close(changes)
		// TODO: signal this channel to stop as well

		for {
			current, meta, err := kv.Get(this.Key, &api.QueryOptions{
				WaitIndex: this.waitIndex,
			})

			if err != nil {
				// TODO: maybe we should retry first?
				log.Debug("failed to monitor key %v: %v", this.Key, err.Error())
				return
			}

			if meta.LastIndex != this.waitIndex {
				this.waitIndex = meta.LastIndex

				changes <- current
			}
		}
	}()

	for {
		select {
		case pair, ok := <-changes:
			if !ok {
				// changes channel closed, consul querying failed
				return
			}
			log.Debug("key %v changed at index %v", this.Key, pair.ModifyIndex)
			this.Changed <- pair
		case <-this.stop:
			return
		}
	}
}

func (this *KeyMonitor) Stop() {
	this.stopOnce.Do(func() {
		close(this.stop)
	})
	<-this.Changed
}
