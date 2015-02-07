package broker

import(
"github.com/hashicorp/consul/api"
"os"
"fmt"
"github.com/golang/glog"
"time"
)

type Broker struct{
    // the unique identifier for this broker service, hostname + pid
    id string
    // the id of the check related to the service registration in consul agent for this service.
    checkId string
    // the id of the active session in consul
    sessionId string

    consulConfig *api.Config
    consulClient *api.Client

    heartbeat *heartbeat
}

func NewBroker() (*Broker, error){
    hostname, err := os.Hostname()
    if err != nil {
        return nil, err
    }

    return &Broker{
        id: fmt.Sprintf("%v-%v", hostname, os.Getpid()),
        consulConfig: api.DefaultConfig(),
    }, nil
}

func (this *Broker) Run(stop chan struct{}) (err error) {
    if err = this.connectToConsul(); err != nil {
        return
    }

    if err = this.announceBroker(); err != nil {
        return
    }

    if err = this.startHeartbeat(); err != nil {
        return
    }
    defer this.heartbeat.Stop()

    if err = this.initSession(); err != nil {
        return
    }
    defer this.destroySession()

    select {
    case <-this.heartbeat.Done:
        glog.V(2).Infof("heartbeat stopped")
        return

    case <-stop:
        glog.V(2).Infof("stop requested")
        return
    }
}

func (this *Broker) connectToConsul() (err error) {
    if this.consulClient, err = api.NewClient(this.consulConfig); err != nil {
        err = fmt.Errorf("failed to connect to consul at %v: %v", this.consulConfig.Address, err.Error())
    }
    return
}

func (this *Broker) announceBroker() (err error) {
    agent := this.consulClient.Agent()

    // register this instance as a service
    if err = agent.ServiceRegister(&api.AgentServiceRegistration{
        ID:   this.id,
        Name: "broker",
        Check: &api.AgentServiceCheck{
            TTL: "1s",
        },
    }); err != nil {
        return fmt.Errorf("failed to register broker at consul agent: %v", err.Error())
    }
    glog.V(2).Info("registered broker at consul")

    // get check id service to heartbeat later
    var checks map[string]*api.AgentCheck
    if checks, err = agent.Checks(); err != nil {
        return fmt.Errorf("failed to query checks from consul agent: %v", err.Error())
    }

    var serviceCheck *api.AgentCheck
    for _, check := range checks {
        if check.ServiceID == this.id {
            serviceCheck = check
        }
    }
    if serviceCheck == nil {
        return fmt.Errorf("failed to find service after registration at consul")
    }

    this.checkId = serviceCheck.CheckID
    return
}

func (this *Broker) initSession() (err error) {
    this.sessionId, err = this.consulClient.Session().Create(&api.SessionEntry{
        Name: fmt.Sprintf("broker-%v-session", this.id),
        Checks: []strings{this.checkId},
        LockDelay: 1 * time.Second,
    })
    return
}

func (this *Broker) destroySession() (error) {
    this.consulClient.Session().Destroy(this.sessionId, nil)
    return nil
}

func (this *Broker) startHeartbeat() (err error) {
    this.heartbeat, err = newHeartbeat(this.consulClient, this.checkId, 100 * time.Millisecond, 1 * time.Second)
    return
}