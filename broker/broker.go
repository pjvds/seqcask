package broker

import (
	"fmt"
	"os"

	"github.com/pjvds/seqcask/cluster"
)

type Broker struct {
	id      string
	session *cluster.Session
}

func NewBroker() (*Broker, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return &Broker{
		id: fmt.Sprintf("%v-%v", hostname, os.Getpid()),
	}, nil
}

func (this *Broker) Run(stop chan struct{}) (err error) {
	session, err := cluster.Join(this.id)
	if err != nil {
		return err
	}

	select {
	case <-session.Lost:
		return fmt.Errorf("cluster session lost")
	case <-stop:
		session.Destroy()
		return
	}
}
