package cluster

import (
	"fmt"
	"time"

	"github.com/hashicorp/consul/api"
)

// Session represents an active session to the cluster.
type Session struct {
	// the unique identifier for this Session service, hostname + pid
	id string
	// the id of the check related to the service registration in consul agent for this service.
	checkId string
	// the id of the active session in consul
	sessionId string

	consulClient *api.Client
	heartbeat    *heartbeater

	Lost    chan struct{}
	destroy chan struct{}
}

func Join(brokerId string) (*Session, error) {
	session := &Session{
		id:      brokerId,
		Lost:    make(chan struct{}),
		destroy: make(chan struct{}),
	}

	log.Infof("connecting to consul")
	if err := session.connect(); err != nil {
		return nil, err
	}

	log.Infof("announcing at cluster")
	if err := session.announce(); err != nil {
		return nil, err
	}

	log.Infof("creating session")
	if err := session.create(); err != nil {
		return nil, err
	}

	go session.do()
	return session, nil
}

func (this *Session) do() {
	defer func() {
		this.heartbeat.Stop()
		this.consulClient.Session().Destroy(this.sessionId, nil)
		close(this.Lost)
	}()

	select {
	case <-this.heartbeat.Done:
		log.Infof("heartbeat stopped")
		return

	case <-this.destroy:
		log.Infof("destroying session")
		return
	}
}

func (this *Session) connect() (err error) {
	config := api.DefaultConfig()

	if this.consulClient, err = api.NewClient(config); err != nil {
		err = fmt.Errorf("failed to connect to consul at %v: %v", config.Address, err.Error())
	}
	return
}

func (this *Session) announce() (err error) {
	agent := this.consulClient.Agent()

	// register this instance as a service
	if err = agent.ServiceRegister(&api.AgentServiceRegistration{
		ID:   this.id,
		Name: "seqcask",
		Check: &api.AgentServiceCheck{
			TTL: "1s",
		},
	}); err != nil {
		return fmt.Errorf("failed to register %v at consul agent: %v", this.id, err.Error())
	}
	log.Infof("registered %v at consul", this.id)

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
	log.Infof("found check id %v for %v", this.checkId, this.id)

	this.heartbeat, err = newHeartbeater(this.consulClient, this.checkId, 100*time.Millisecond, 1*time.Second)
	if err != nil {
		return fmt.Errorf("failed to initialize heartbeat: %v", err.Error())
	}

	log.Infof("heartbeat initialized")
	return
}

func (this *Session) create() (err error) {
	this.sessionId, _, err = this.consulClient.Session().Create(&api.SessionEntry{
		Name:      fmt.Sprintf("Session-%v-session", this.id),
		Checks:    []string{this.checkId},
		LockDelay: 1 * time.Second,
	}, nil)

	if err != nil {
		return fmt.Errorf("failed to create consul session: %v", err.Error())
	}
	return
}

func (this *Session) Destroy() {
	close(this.destroy)

	<-this.Lost
	return
}
