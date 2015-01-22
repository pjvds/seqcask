package seqcask

import "math/rand"

type RandomValueGenerator struct {
	valueSize int
	Values    chan []byte
	stop      chan struct{}
}

func NewRandomValueGenerator(valueSize int) *RandomValueGenerator {
	generator := &RandomValueGenerator{
		valueSize: valueSize,
		Values:    make(chan []byte, 1024),
		stop:      make(chan struct{}),
	}
	go generator.do()
	return generator
}

func (this *RandomValueGenerator) do() {
	defer close(this.Values)

	for {
		value := make([]byte, this.valueSize, this.valueSize)
		for index := range value {
			value[index] = byte(rand.Intn(255))
		}

		select {
		case this.Values <- value:
		case <-this.stop:
			return
		}
	}
}

func (this *RandomValueGenerator) Stop() {
	close(this.stop)
}
