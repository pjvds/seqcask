package cluster

import (
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
)

// Tries to keep an check passing by heartbeating to the consul server
type heartbeater struct {
	consul   *api.Client
	checkId  string
	interval time.Duration
	timeout  time.Duration

	Done chan error

	stop     chan bool
	stopOnce sync.Once
}

func newHeartbeater(consul *api.Client, checkId string, interval time.Duration, timeout time.Duration) (h *heartbeater, err error) {
	h = &heartbeater{
		consul:   consul,
		checkId:  checkId,
		interval: interval,
		timeout:  timeout,

		Done: make(chan error),
		stop: make(chan bool),
	}
	// try to execute first heartbeat
	if err = h.passTTL(); err != nil {
		h = nil
		return
	}

	// we succesfully executed a single heartbeat, start beating continuesly
	go h.doHeartbeat()
	return
}

// execute a single heartbeat
func (this *heartbeater) passTTL() (err error) {
	agent := this.consul.Agent()

	if err = agent.PassTTL(this.checkId, "heathbeat running"); err != nil {
		log.Debug("failed to pass ttl for check id %v: ", this.checkId, err.Error())
	}
	return
}

func (this *heartbeater) failTTL() error {
	agent := this.consul.Agent()
	return agent.FailTTL(this.checkId, "heathbeat stopped")
}

func (this *heartbeater) doHeartbeat() {
	defer func() {
		log.Debug("heartbeater stopped")
		close(this.Done)
	}()

	for {
		select {
		case <-time.After(this.interval):
			if err := this.passTTL(); err != nil {
				log.Infof("heartbeat for check id %v failed: %v", this.checkId, err.Error())
				return
			}
			break
		case <-this.stop:
			log.Debug("stop requested")
			this.failTTL()
			return
		}
	}
}

func (this *heartbeater) Stop() {
	this.stopOnce.Do(func() {
		close(this.stop)
	})
	<-this.Done
}
