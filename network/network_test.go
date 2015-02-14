package network

import (
	"flag"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func init() {
	flag.Set("v", "2")
}

func TestRoundtrip(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:5678")
	if err != nil {
		t.Fatalf("error: %v", err.Error())
	}
	defer listener.Close()

	handler := HandleFunc(func(request *Request) (*Response, error) {
		return &Response{
			Id:      request.Id,
			Payload: request.Payload,
		}, nil
	})
	server := NewServer(handler)
	go server.Serve(listener)

	runtime.Gosched()
	time.Sleep(1 * time.Second)

	client, err := Dial("localhost:5678")
	assert.Nil(t, err)
	defer client.Close()

	request := &Request{
		Payload: []byte("hello world"),
	}
	response, err := client.Request(request)

	assert.Nil(t, err)
	assert.NotNil(t, response)
	assert.Equal(t, response.Id, request.Id)
	assert.Equal(t, response.Payload, request.Payload)
}

func BenchmarkRoundtrip(b *testing.B) {
	listener, err := net.Listen("tcp", "localhost:5678")
	if err != nil {
		b.Fatalf("error: %v", err.Error())
	}
	defer listener.Close()

	handler := HandleFunc(func(request *Request) (*Response, error) {
		return &Response{
			Id:      request.Id,
			Payload: request.Payload,
		}, nil
	})
	server := NewServer(handler)
	go server.Serve(listener)

	runtime.Gosched()
	time.Sleep(1 * time.Second)

	client, err := Dial("localhost:5678")
	if err != nil {
		b.Fatalf("client error: %v", err.Error())
	}
	defer client.Close()

	request := &Request{
		Payload: []byte("hello world"),
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		_, err := client.Request(request)

		if err != nil {
			b.Fatalf("error: %v", err.Error())
		}

		b.SetBytes(int64(len(request.Payload)))
	}
}

func BenchmarkRoundtrip6(b *testing.B) {
	listener, err := net.Listen("tcp", "localhost:5678")
	if err != nil {
		b.Fatalf("error: %v", err.Error())
	}
	defer listener.Close()

	handler := HandleFunc(func(request *Request) (*Response, error) {
		return &Response{
			Id:      request.Id,
			Payload: request.Payload,
		}, nil
	})
	server := NewServer(handler)
	go server.Serve(listener)

	runtime.Gosched()
	time.Sleep(1 * time.Second)

	client, err := Dial("localhost:5678")
	if err != nil {
		b.Fatalf("client error: %v", err.Error())
	}
	defer client.Close()

	request := &Request{
		Payload: []byte("hello world"),
	}

	b.ResetTimer()
	work := new(sync.WaitGroup)
	requests := make(chan *Request)

	for worker := 0; worker < 6; worker++ {
		work.Add(1)

		go func() {
			defer work.Done()
			for request := range requests {
				_, err := client.Request(request)

				if err != nil {
					b.Fatalf("error: %v", err.Error())
				}
			}
		}()
	}

	for n := 0; n < b.N; n++ {
		requests <- request
		b.SetBytes(int64(len(request.Payload)))
	}
	close(requests)

	work.Wait()
}
