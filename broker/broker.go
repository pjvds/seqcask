package broker

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/rep"
	"github.com/gdamore/mangos/transport/ipc"
	"github.com/gdamore/mangos/transport/tcp"
	"github.com/pjvds/seqcask/cluster"
	"github.com/pjvds/seqcask/request"
	"github.com/pjvds/seqcask/response"
	"github.com/pjvds/seqcask/storage"
	"github.com/rcrowley/go-metrics"
)

type Broker struct {
	id               string
	session          *cluster.Session
	clientApiAddress string

	socket mangos.Socket

	partitions map[uint16]chan *PartitionWriteRequest
}

func NewBroker() (*Broker, error) {
	directory, _ := ioutil.TempDir("", "bitcast_test_")
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	partitions := make(map[uint16]chan *PartitionWriteRequest)

	for i := uint16(1); i < 4; i++ {
		filename := filepath.Join(directory, fmt.Sprintf("%v.data", i))
		store := storage.MustCreate(filename, 128*1000*1000)
		requests := make(chan *PartitionWriteRequest)

		NewTopicPartitionWriter("test", i, requests, store)
		partitions[i] = requests
	}

	return &Broker{
		id:               fmt.Sprintf("%v-%v", hostname, os.Getpid()),
		clientApiAddress: "tcp://127.0.0.1:40899",
		partitions:       partitions,
	}, nil
}

func (this *Broker) Run(stop chan struct{}) (err error) {
	session, err := cluster.Join(this.id)
	if err != nil {
		return err
	}

	clientApiErr := make(chan error)
	go this.runClientApi()

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

	for n := 0; n < 10; n++ {
		go this.requestWorker(socket)
	}

	return nil
}

func (this *Broker) handleRequest(socket mangos.Socket, message *mangos.Message, pool *sync.Pool) {
	//defer message.Free()

	if message.Body[0] == request.T_Append {
		topicLength := int(message.Body[1])
		//topic := string(message.Body[2 : 2+topicLength])
		partition := binary.LittleEndian.Uint16(message.Body[2+topicLength:])

		messages := message.Body[2+topicLength+2:]
		writeMessages := make([][]byte, 0)

		for i := 0; i < len(messages); {
			length := binary.LittleEndian.Uint32(messages[i:])
			i += 4

			message := messages[i : i+int(length)]
			writeMessages = append(writeMessages, message)

			i += int(length)
		}

		writeRequest := NewPartitionWriteRequest(writeMessages)
		this.partitions[partition] <- writeRequest

		err := writeRequest.WaitForDone()

		if err != nil {
			message.Body = append([]byte{0x01}, []byte(err.Error())...)
		} else {
			message.Body = []byte{0x00}
		}
		socket.SetOption(mangos.OptionSendDeadline, 1*time.Second)
		if err := socket.SendMsg(message); err != nil {
			log.WithField("error", err).Warn("send message error")
		}
	} else {
		message.Body = append([]byte{response.T_ERROR}, []byte("unknown request type")...)
		socket.SendMsg(message)
	}
}

func (this *Broker) requestWorker(socket mangos.Socket) {
	var message *mangos.Message
	var err error

	pool := &sync.Pool{
		New: func() interface{} {
			return storage.NewWriteBatch()
		},
	}

	requestCount := metrics.NewRegisteredCounter("broker.request.count", metrics.DefaultRegistry)
	for {
		//log.Info("waiting for message")
		if message, err = socket.RecvMsg(); err != nil {
			log.Info("request worker failed: %v", err.Error())
			return
		}
		requestCount.Inc(1)

		go this.handleRequest(socket, message, pool)
	}
}
