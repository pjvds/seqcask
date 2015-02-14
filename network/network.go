package network

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"

	"github.com/golang/glog"
)

type RequestType byte
type ResponseType byte

const (
	MagicStart = byte(0x42)
)

const (
	ReqTypeAppendStream = RequestType(iota)
)

const (
	ResTypeOk = ResponseType(iota)
	ResTypeErrpr
)

type RequestContext struct {
	Request  *Request
	Response *Response
	Error    error

	done chan struct{}
}

func NewRequestContext(request *Request) *RequestContext {
	return &RequestContext{
		Request:  request,
		Response: nil,
		Error:    nil,

		done: make(chan struct{}),
	}
}

type Request struct {
	Id      uint32
	Payload []byte
}

type Response struct {
	Id      uint32
	Payload []byte
}

type Handler interface {
	Handle(request *Request) (*Response, error)
}

type HandleFunc func(request *Request) (*Response, error)

func (this HandleFunc) Handle(request *Request) (*Response, error) {
	return this(request)
}

type Client struct {
	conn     net.Conn
	requests chan *RequestContext

	inflight map[uint32]*RequestContext
}

func Dial(address string) (*Client, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	client := &Client{
		conn:     conn,
		inflight: make(map[uint32]*RequestContext),
		requests: make(chan *RequestContext),
	}
	go client.do()

	return client, nil
}

func (this *Client) Close() error {
	return this.conn.Close()
}

func (this *Client) Request(request *Request) (*Response, error) {
	ctx := NewRequestContext(request)
	this.requests <- ctx

	<-ctx.done

	return ctx.Response, ctx.Error
}

func (this *Client) do() {
	id := uint32(0)
	buffer := new(bytes.Buffer)

	send := make(chan *RequestContext)
	responses := make(chan *Response)

	go func() {
		for ctx := range this.requests {
			buffer.Truncate(0)

			request := ctx.Request
			request.Id = id
			size := uint32(len(request.Payload))

			glog.V(2).Infof("preparing to send request %v, msg size %v", request.Id, size)

			buffer.WriteByte(MagicStart)
			binary.Write(buffer, binary.LittleEndian, request.Id)
			binary.Write(buffer, binary.LittleEndian, size)
			buffer.Write(request.Payload)

			if _, err := this.conn.Write(buffer.Bytes()); err != nil {
				glog.V(2).Info("send error: %v", err.Error())
				ctx.Error = err
				close(ctx.done)

				continue
			}

			send <- ctx
			id++
		}
	}()

	go func() {
		defer close(responses)
		reader := bufio.NewReader(this.conn)

		for {
			magic, err := reader.ReadByte()
			if err != nil {
				glog.V(2).Infof("read error: %v", err.Error())
				return
			}
			if magic != MagicStart {
				continue
			}

			var id uint32
			if err := binary.Read(reader, binary.LittleEndian, &id); err != nil {
				glog.V(2).Infof("read error: %v", err.Error())
				continue
			}

			var length uint32
			if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
				glog.V(2).Infof("read error: %v", err.Error())
				continue
			}

			payload := make([]byte, length, length)
			if read, err := reader.Read(payload); err != nil {
				glog.V(2).Infof("read error: %v", err.Error())
				continue
			} else if uint32(read) < length {
				glog.V(2).Infof("read error: read too short: %v read, %v expect", read, length)
				continue
			} else if uint32(read) > length {
				glog.V(2).Infof("read error: read too long: %v read, %v expect", read, length)
				continue
			}

			responses <- &Response{
				Id:      id,
				Payload: payload,
			}
		}
	}()

	for {
		select {
		case send := <-send:
			this.inflight[send.Request.Id] = send
		case response := <-responses:
			if response == nil {
				return
			}

			context, ok := this.inflight[response.Id]
			if !ok {
				glog.V(2).Infof("no inflight request found id %v", response.Id)
				continue
			}
			delete(this.inflight, response.Id)

			context.Response = response
			close(context.done)
		}
	}
}

type Server struct {
	handler Handler
}

func NewServer(handler Handler) *Server {
	return &Server{
		handler: handler,
	}
}

func ListenAndServe(address string, handler Handler) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	server := NewServer(handler)
	return server.Serve(listener)
}

func (this *Server) Serve(listener net.Listener) error {
	for {
		connection, err := listener.Accept()
		if err != nil {
			return err
		}

		glog.V(2).Info("connection accepted")
		NewConnectionServer(connection, this.handler)
	}
}

type ConnectionServer struct {
	conn    net.Conn
	handler Handler
}

func NewConnectionServer(conn net.Conn, handler Handler) *ConnectionServer {
	server := &ConnectionServer{
		conn:    conn,
		handler: handler,
	}
	go server.do()

	return server
}

func (this *ConnectionServer) readNextRequest(reader *bufio.Reader) (*Request, error) {
	for {
		magic, err := reader.ReadByte()
		if err != nil {
			glog.V(2).Infof("magic byte read error: %v", err.Error())
			return nil, err
		}
		if magic != MagicStart {
			glog.V(2).Infof("first by was not magic start")
			continue
		}

		var id uint32
		if err := binary.Read(reader, binary.LittleEndian, &id); err != nil {
			glog.V(2).Infof("id read error: %v", err.Error())
			continue
		}

		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			glog.V(2).Infof("length read error: %v", err.Error())
			continue
		}
		glog.V(2).Infof("processing incomming request %v, size %v", id, length)

		if length <= 1 {
			glog.V(2).Infof("invalid request length")
			continue
		}

		payload := make([]byte, length, length)
		if read, err := reader.Read(payload); err != nil {
			glog.V(2).Infof("payload read error: %v", err.Error())
			continue
		} else if uint32(read) < length {
			glog.V(2).Infof("payload read error: read too short")
			continue
		} else if uint32(read) > length {
			glog.V(2).Infof("payload read error: read too long")
			continue
		}

		return &Request{
			Id:      id,
			Payload: payload,
		}, nil
	}
}

func (this *ConnectionServer) do() {
	defer func() {
		this.conn.Close()
	}()

	responses := make(chan *Response, 50)
	closed := make(chan struct{})

	go func() {
		reader := bufio.NewReader(this.conn)
		defer close(responses)

		for {
			request, err := this.readNextRequest(reader)
			if err != nil {
				glog.V(2).Infof("failed to read request: %v", err.Error())
				return
			}

			go func(request *Request) {
				if response, err := this.handler.Handle(request); err != nil {
					glog.V(2).Infof("failed to handle request: %v", err.Error())
				} else {
					select {
					case responses <- response:
						glog.V(2).Infof("response %v dispatched", response.Id)
					case <-closed:
						glog.V(2).Infof("connection server closed")
					}
				}
			}(request)
		}
	}()

	writer := bufio.NewWriter(this.conn)

	buffer := new(bytes.Buffer)
	for response := range responses {
		buffer.WriteByte(MagicStart)
		binary.Write(buffer, binary.LittleEndian, response.Id)
		binary.Write(buffer, binary.LittleEndian, uint32(len(response.Payload)))
		buffer.Write(response.Payload)

		buffer.WriteTo(this.conn)
		writer.Flush()

		buffer.Truncate(0)
	}
}
