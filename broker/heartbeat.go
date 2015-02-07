package broker

import (
    "sync"
    "time"

    "github.com/hashicorp/consul/api"
    "github.com/golang/glog"
)

// Tries to keep an check passing by heartbeating to the consul server
type heartbeat struct {
    consul   *api.Client
    checkId  string
    interval time.Duration
    timeout  time.Duration

    Done chan error

    stop     chan bool
    stopOnce sync.Once
}

func newHeartbeat(consul *api.Client, checkId string, interval time.Duration, timeout time.Duration) (h *heartbeat, err error) {
    h = &heartbeat{
        consul:   consul,
        checkId:  checkId,
        interval: interval,
        timeout:  timeout,

        Done: make(chan error),
        stop: make(chan bool),
    }
    // try to execute first heartbeat
    if err = h.passTTL(); err != nil {
        h= nil
        return
    }

    // we succesfully executed a single heartbeat, start beating continuesly
    go h.doHeartbeat()
    return
}

// execute a single heartbeat
func (this *heartbeat) passTTL() (err error) {
    agent := this.consul.Agent()

    if err = agent.PassTTL(this.checkId, "heathbeat running"); err != nil {
        glog.V(2).Infof("failed to pass ttl for check id %v: ", this.checkId, err.Error())
    }
    return
}

func (this *heartbeat) failTTL() error {
    agent := this.consul.Agent()
    return agent.FailTTL(this.checkId, "heathbeat stopped")
}

func (this *heartbeat) doHeartbeat() {
    defer func() {
        close(this.Done)
    }()

    for {
        select {
        case <-time.After(this.interval):
            if err := this.passTTL(); err != nil {
                glog.V(2).Infof("heartbeat for check id %v failed: %v", this.checkId, err.Error())
                return
            }
            break
        case <-this.stop:
            glog.V(2).Infof("heartbeat stop requested")
            this.failTTL()
            return
        }
    }
}

func (this *heartbeat) Stop() {
    this.stopOnce.Do(func() {
        close(this.stop)
    })
    <-this.Done
}