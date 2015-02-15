package network

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/golang/glog"
	"github.com/pjvds/randombytes"
)

// func init() {
// 	flag.Set("v", "2")
// }

// func TestRoundtrip(t *testing.T) {
// 	port := 5000 + (time.Now().Nanosecond() % 1000)
// 	address := fmt.Sprintf("localhost:%v", port)

// 	listener, err := net.Listen("tcp", address)
// 	if err != nil {
// 		t.Fatalf("error: %v", err.Error())
// 	}
// 	defer listener.Close()

// 	handler := HandleFunc(func(request *Request) (*Response, error) {
// 		return &Response{
// 			Id:      request.Id,
// 			Payload: request.Payload,
// 		}, nil
// 	})
// 	server := NewServer(handler)
// 	go server.Serve(listener)

// 	runtime.Gosched()
// 	time.Sleep(1 * time.Second)

// 	client, err := Dial(address)
// 	assert.Nil(t, err)
// 	defer client.Close()

// 	request := &Request{
// 		Payload: []byte("hello world"),
// 	}
// 	response, err := client.Request(request)

// 	assert.Nil(t, err)
// 	assert.NotNil(t, response)
// 	assert.Equal(t, response.Id, request.Id)
// 	assert.Equal(t, response.Payload, request.Payload)
// }

// func BenchmarkRoundtrip(b *testing.B) {
// 	port := 5000 + (time.Now().Nanosecond() % 1000)
// 	address := fmt.Sprintf("localhost:%v", port)

// 	listener, err := net.Listen("tcp", address)
// 	if err != nil {
// 		b.Fatalf("error: %v", err.Error())
// 	}
// 	defer listener.Close()

// 	handler := HandleFunc(func(request *Request) (*Response, error) {
// 		return &Response{
// 			Id:      request.Id,
// 			Payload: request.Payload,
// 		}, nil
// 	})
// 	server := NewServer(handler)
// 	go server.Serve(listener)

// 	runtime.Gosched()
// 	time.Sleep(1 * time.Second)

// 	client, err := Dial(address)
// 	if err != nil {
// 		b.Fatalf("client error: %v", err.Error())
// 	}
// 	defer client.Close()

// 	request := &Request{
// 		Payload: []byte("hello world"),
// 	}

// 	b.ResetTimer()

// 	for n := 0; n < b.N; n++ {
// 		_, err := client.Request(request)

// 		if err != nil {
// 			b.Fatalf("error: %v", err.Error())
// 		}

// 		b.SetBytes(int64(len(request.Payload)))
// 	}
// }

func BenchmarkRoundtrip6(b *testing.B) {
	glog.Infof("starting benchmark, n=%v", b.N)

	port := 5000 + (time.Now().Nanosecond() % 1000)
	address := fmt.Sprintf("localhost:%v", port)

	listener, err := net.Listen("tcp", address)
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

	client, err := Dial(address)
	if err != nil {
		b.Fatalf("client error: %v", err.Error())
	}
	defer client.Close()

	b.ResetTimer()
	work := new(sync.WaitGroup)
	requests := make(chan *Request)

	for worker := 0; worker < 25; worker++ {
		work.Add(1)

		go func(worker int) {
			defer func() {
				glog.Infof("worker %v done", worker)
				work.Done()
			}()

			for request := range requests {
				_, err := client.Request(request)

				if err != nil {
					b.Fatalf("request %v failed: %v", request.Id, err.Error())
				}
			}
		}(worker)
	}

	payload := randombytes.Make(200)

	for n := 0; n < b.N; n++ {
		request := &Request{
			Payload: payload,
		}
		requests <- request
		b.SetBytes(int64(len(request.Payload)))
	}

	glog.Infof("benchmark finishing, n=%v", b.N)
	close(requests)

	work.Wait()
}
