package broker

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/rep"
	"github.com/gdamore/mangos/transport/ipc"
	"github.com/gdamore/mangos/transport/tcp"
	"github.com/pjvds/seqcask/cluster"
	"github.com/pjvds/seqcask/request"
	"github.com/pjvds/seqcask/response"
	"github.com/pjvds/seqcask/storage"
)

type Broker struct {
	id               string
	session          *cluster.Session
	clientApiAddress string

	socket mangos.Socket
}

func NewBroker() (*Broker, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return &Broker{
		id:               fmt.Sprintf("%v-%v", hostname, os.Getpid()),
		clientApiAddress: "tcp://127.0.0.1:40899",
	}, nil
}

func (this *Broker) Run(stop chan struct{}) (err error) {
	session, err := cluster.Join(this.id)
	if err != nil {
		return err
	}

	clientApiErr := make(chan error)
	go func() {
		clientApiErr <- this.runClientApi()
	}()

	for {
		select {
		case err := <-clientApiErr:
			log.Errorf("client api failed: %v", err.Error())
			session.Destroy()
		case <-session.Lost:
			return fmt.Errorf("cluster session lost")
		case <-stop:
			session.Destroy()
		}
	}
}

func (this *Broker) runClientApi() error {

	var socket mangos.Socket
	var err error

	if socket, err = rep.NewSocket(); err != nil {
		return err
	}

	if err = socket.SetOption(mangos.OptionRaw, true); err != nil {
		return err
	}

	socket.AddTransport(ipc.NewTransport())
	socket.AddTransport(tcp.NewTransport())
	if err = socket.Listen(this.clientApiAddress); err != nil {
		return err
	}
	log.WithField("address", this.clientApiAddress).Info("client api listening")

	this.requestWorker(socket)
	return nil
}

func (this *Broker) requestWorker(socket mangos.Socket) {
	var message *mangos.Message
	var err error
	batch := storage.NewWriteBatch()

	for {
		log.Info("waiting for message")
		if message, err = socket.RecvMsg(); err != nil {
			log.Info("request worker failed: %v", err.Error())
			return
		}
		log.Info("request message received")

		if message.Body[0] == request.T_Append {
			topicLength := int(message.Body[1])
			//topic := string(message.Body[2 : 2+topicLength])
			//partition := binary.LittleEndian.Uint16(message.Body[2+topicLength:])

			messages := message.Body[2+topicLength+2:]

			for i := 0; i < len(messages); {
				length := binary.LittleEndian.Uint32(messages[i:])
				i += 4

				message := messages[i : i+int(length)]
				batch.Put(message)

				// log.Infof("message %v/%v: %v", topic, partition, string(message))

				i += int(length)
			}

			message.Body = []byte{0x00}

			socket.SetOption(mangos.OptionSendDeadline, 1*time.Second)
			if err := socket.SendMsg(message); err != nil {
				log.WithField("error", err).Warn("send message error")
			}
			log.Info("reply send")

			batch.Reset()
		} else {
			message.Body = append([]byte{response.T_ERROR}, []byte("unknown request type")...)
			socket.SendMsg(message)
		}
	}
}
