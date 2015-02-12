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

type publishRequest struct {
	topic     string
	partition uint16
	body      []byte

	done chan error
}

func (this publishRequest) WaitForResult() error {
	return <-this.done
}

type TopicPartition struct {
	Topic     string
	Partition uint16
}

type Producer struct {
	workers    sync.WaitGroup
	partitions map[TopicPartition]chan publishRequest
}

func NewProducer(brokerAddress string) (*Producer, error) {
	producer := &Producer{
		partitions: make(map[TopicPartition]chan publishRequest),
	}

	socket, err := req.NewSocket()
	if err != nil {
		return nil, err
	}

	socket.AddTransport(ipc.NewTransport())
	socket.AddTransport(tcp.NewTransport())

	if err := socket.Dial(brokerAddress); err != nil {
		return nil, err
	}

	testTopic := make(chan publishRequest)
	producer.partitions[TopicPartition{"test", 1}] = testTopic

	go producer.worker(socket, "test", 1, testTopic)

	return producer, nil
}

func (this *Producer) Publish(topic string, partition uint16, body []byte) chan error {
	requests, ok := this.partitions[TopicPartition{topic, partition}]
	done := make(chan error, 1)

	if !ok {
		done <- fmt.Errorf("no found")
		return done
	}

	request := publishRequest{
		topic:     topic,
		partition: partition,
		body:      body,

		done: done,
	}
	requests <- request
	return done
}

func (this *Producer) worker(socket mangos.Socket, topic string, partition uint16, requests chan publishRequest) {
	locallog := log.WithFields(logrus.Fields{
		"topic":     topic,
		"partition": partition,
	})

	// locallog.Infof("started")

	var buffer bytes.Buffer

	var request publishRequest
	var ok bool

	maxDataSize := int(1e6) // 5mb

	queue := make([]publishRequest, 0)

	// write header
	buffer.WriteByte(0)
	buffer.WriteByte(byte(len(topic)))
	buffer.WriteString(topic)
	binary.Write(&buffer, binary.LittleEndian, partition)

	// remember data starting point
	dataStart := buffer.Len()

	for {
		// wait for first request
		if request, ok = <-requests; !ok {
			// request channel closed
			return
		}
		// locallog.Info("request received")

		// truncate to data starting point
		buffer.Truncate(dataStart)
		queue = queue[0:0]

		// write data
		queue = append(queue, request)
		binary.Write(&buffer, binary.LittleEndian, uint32(len(request.body)))
		buffer.Write(request.body)

		linger := time.After(150 * time.Millisecond)

		flush := false
		for !flush && buffer.Len() < maxDataSize {
			select {
			case request = <-requests:
				// locallog.Info("request received")
				queue = append(queue, request)
				binary.Write(&buffer, binary.LittleEndian, uint32(len(request.body)))
				buffer.Write(request.body)
			case <-linger:
				// the scheduler has nothing for us
				flush = true
			}
		}

		// send request to broker
		// locallog.Info("message sending")
		if err := socket.SetOption(mangos.OptionSendDeadline, 1*time.Second); err != nil {
			locallog.WithFields(logrus.Fields{
				"error":  err,
				"option": mangos.OptionSendDeadline}).Error("failed to set option")
			continue
		}

		if err := socket.Send(buffer.Bytes()); err != nil {
			locallog.WithField("error", err).Error("send failed")

			// failed to send
			for _, request = range queue {
				request.done <- err
			}
			continue
		}

		// locallog.Info("message receiving")
		if err := socket.SetOption(mangos.OptionRecvDeadline, 5*time.Second); err != nil {
			locallog.WithFields(logrus.Fields{
				"error":  err,
				"option": mangos.OptionRecvDeadline}).Error("failed to set option")
			continue
		}

		if reply, err := socket.Recv(); err != nil {
			locallog.WithField("error", err).Error("failed to receive")

			// failed to receive
			for _, request = range queue {
				request.done <- err
			}
			continue
		} else {
			//locallog.Info("message parsing reply")

			if len(reply) == 0 {
				// received empty reply
				err := fmt.Errorf("invalid socket reply length 0")
				for _, request = range queue {
					request.done <- err
				}
			} else if reply[0] == response.T_OK {
				// received ok!
				for _, request = range queue {
					request.done <- nil
				}
			} else if reply[0] == response.T_ERROR {
				// received error
				err := errors.New(string(reply[1:]))
				for _, request = range queue {
					request.done <- err
				}
			} else {
				// received unknown reply
				err := fmt.Errorf("unknown reply type %v", reply[0])
				for _, request = range queue {
					request.done <- err
				}
			}
		}
	}
}
