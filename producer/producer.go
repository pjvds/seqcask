package producer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gdamore/mangos"
	"github.com/gdamore/mangos/protocol/req"
	"github.com/gdamore/mangos/transport/ipc"
	"github.com/gdamore/mangos/transport/tcp"
	"github.com/pjvds/seqcask/response"
)

type TopicPartition struct {
	Topic     string
	Partition uint16
}

type Producer struct {
	workers    sync.WaitGroup
	partitions map[uint16]*partition
}

type partition struct {
	topic     string
	partition uint16

	requests chan *PublishResult

	socket mangos.Socket

	Done chan struct{}
}

func newPartition(brokerAddress string, topic string, part uint16) (*partition, error) {
	socket, err := req.NewSocket()
	if err != nil {
		return nil, err
	}

	socket.AddTransport(ipc.NewTransport())
	socket.AddTransport(tcp.NewTransport())

	if err := socket.Dial(brokerAddress); err != nil {
		return nil, err
	}

	p := &partition{
		topic:     topic,
		partition: part,

		requests: make(chan *PublishResult),
		Done:     make(chan struct{}),

		socket: socket,
	}
	go p.do()

	return p, nil
}

func (this *partition) do() {
	defer close(this.Done)

	locallog := log.WithFields(logrus.Fields{
		"topic":     this.topic,
		"partition": this.partition,
	})

	locallog.Infof("started")

	var buffer bytes.Buffer

	maxDataSize := int(128e3) // 64 kb

	queue := make([]*PublishResult, 0)

	// write header
	buffer.WriteByte(0)
	buffer.WriteByte(byte(len(this.topic)))
	buffer.WriteString(this.topic)
	binary.Write(&buffer, binary.LittleEndian, this.partition)

	// remember data starting point
	dataStart := buffer.Len()

	for {
		var request *PublishResult
		var ok bool

		// wait for first request
		if request, ok = <-this.requests; !ok {
			// request channel closed
			return
		}

		// truncate to data starting point
		buffer.Truncate(dataStart)
		queue = queue[0:0]

		// write data
		queue = append(queue, request)
		binary.Write(&buffer, binary.LittleEndian, uint32(len(request.Body)))
		buffer.Write(request.Body)

		flush := false
		for !flush && buffer.Len() < maxDataSize {
			select {
			case request = <-this.requests:
				// locallog.Info("request received")
				queue = append(queue, request)
				binary.Write(&buffer, binary.LittleEndian, uint32(len(request.Body)))
				buffer.Write(request.Body)
				//case <-linger:
			default:
				// the scheduler has nothing for us
				flush = true
			}
		}

		// send request to broker
		// locallog.Info("message sending")
		if err := this.socket.SetOption(mangos.OptionSendDeadline, 250*time.Second); err != nil {
			locallog.WithFields(logrus.Fields{
				"error":  err,
				"option": mangos.OptionSendDeadline}).Error("failed to set option")
			continue
		}

		if err := this.socket.Send(buffer.Bytes()); err != nil {
			locallog.WithField("error", err).Error("send failed")

			// failed to send
			for _, request = range queue {
				request.report(err)
			}
			continue
		}

		// locallog.Info("message receiving")
		if err := this.socket.SetOption(mangos.OptionRecvDeadline, 1*time.Second); err != nil {
			locallog.WithFields(logrus.Fields{
				"error":  err,
				"option": mangos.OptionRecvDeadline}).Error("failed to set option")
			continue
		}

		if reply, err := this.socket.Recv(); err != nil {
			locallog.WithField("error", err).Error("failed to receive")

			// failed to receive
			for _, request = range queue {
				request.report(err)
			}
			continue
		} else {
			if len(reply) == 0 {
				// received empty reply
				err := fmt.Errorf("invalid socket reply length 0")
				for _, request = range queue {
					request.report(err)
				}
			} else if reply[0] == response.T_OK {
				// received ok!
				for _, request = range queue {
					request.report(nil)
				}
			} else if reply[0] == response.T_ERROR {
				// received error
				err := errors.New(string(reply[1:]))
				for _, request = range queue {
					request.report(err)
				}
			} else {
				// received unknown reply
				err := fmt.Errorf("unknown reply type %v", reply[0])
				for _, request = range queue {
					request.report(err)
				}
			}
		}
	}
}

func (this *partition) Publish(request *PublishResult) {
	this.requests <- request
}

func NewProducer(brokerAddress string) (*Producer, error) {
	producer := &Producer{
		partitions: make(map[uint16]*partition),
	}

	p1, err := newPartition(brokerAddress, "test", 1)
	if err != nil {
		return nil, err
	}
	producer.partitions[1] = p1

	p2, err := newPartition(brokerAddress, "test", 2)
	if err != nil {
		return nil, err
	}
	producer.partitions[2] = p2

	p3, err := newPartition(brokerAddress, "test", 3)
	if err != nil {
		return nil, err
	}
	producer.partitions[3] = p3
	return producer, nil
}

type PublishResult struct {
	Topic     string
	Partition uint16
	Body      []byte

	Error error

	done chan struct{}
}

func (this *PublishResult) report(err error) {
	this.Error = err
	close(this.done)
}

func (this *PublishResult) WaitForDone() error {
	<-this.done
	return this.Error
}

func (this *Producer) Publish(topic string, part uint16, body []byte) *PublishResult {
	partition, ok := this.partitions[part]
	request := &PublishResult{
		Topic:     topic,
		Partition: part,
		Body:      body,

		done: make(chan struct{}),
	}

	if !ok {
		request.report(fmt.Errorf("no found"))
		return request
	}

	partition.Publish(request)
	return request
}
