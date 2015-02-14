package broker

import "github.com/pjvds/seqcask/storage"

type PartitionWriteRequest struct {
	Messages [][]byte

	Error error
	done  chan struct{}
}

func NewPartitionWriteRequest(messages [][]byte) *PartitionWriteRequest {
	return &PartitionWriteRequest{
		Messages: messages,
		Error:    nil,
		done:     make(chan struct{}),
	}
}

func (this *PartitionWriteRequest) report(err error) {
	this.Error = err
	close(this.done)
}

func (this *PartitionWriteRequest) WaitForDone() error {
	<-this.done
	return this.Error
}

type TopicPartitionWriter struct {
	topic     string
	partition uint16
	requests  chan *PartitionWriteRequest
	store     *storage.Seqcask

	Error error
	Done  chan struct{}
}

func NewTopicPartitionWriter(topic string, partition uint16, requests chan *PartitionWriteRequest, store *storage.Seqcask) *TopicPartitionWriter {
	writer := &TopicPartitionWriter{
		topic:     topic,
		partition: partition,
		requests:  requests,
		store:     store,

		Error: nil,
		Done:  make(chan struct{}),
	}
	go writer.do()

	return writer
}

func (this *TopicPartitionWriter) do() {
	defer close(this.Done)

	var request *PartitionWriteRequest
	var ok bool
	var requests []*PartitionWriteRequest

	batch := storage.NewWriteBatch()

	for {
		if request, ok = <-this.requests; !ok {
			return
		}

		batch.Put(request.Messages...)
		requests = append(requests, request)

		var flush bool
		for !flush && batch.DataSize() < 5e6 {
			select {
			case request, ok = <-this.requests:
				if ok {
					batch.Put(request.Messages...)
					requests = append(requests, request)
				}
			default:
				flush = true
			}
		}

		err := this.store.Write(batch)
		for _, request = range requests {
			request.report(err)
		}

		batch.Reset()
		requests = requests[0:0]
	}
}
