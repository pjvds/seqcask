package network

import (
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func init() {
	flag.Set("v", "2")
}

func TestRoundtrip(t *testing.T) {
	handler := HandleFunc(func(request *Request) (*Response, error) {
		return &Response{
			Id:      request.Id,
			Payload: request.Payload,
		}, nil
	})
	go func() {
		err := ListenAndServe("localhost:5678", handler)
		assert.Nil(t, err)
	}()
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
