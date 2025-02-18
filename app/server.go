package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/codecrafters-io/kafka-starter-go/app/request/api"
	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

func Receive(c net.Conn) ([]byte, error) {
	var message_size uint32
	err := binary.Read(c, binary.BigEndian, &message_size)
	if err != nil {
		return nil, err
	}

	data := make([]byte, message_size)
	_, err = c.Read(data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func Send(c net.Conn, b []byte) error {
	binary.Write(c, binary.BigEndian, int32(len(b)))
	binary.Write(c, binary.BigEndian, b)

	return nil
}

func handleConnection(c net.Conn) {
	defer c.Close()

	for {
		data, err := Receive(c)
		if err != nil {
			fmt.Printf("Error receiving data: %s\n", err.Error())
			os.Exit(1)
		}

		reqHeader := &request.RequestHeader{}
		parser := decoder.NewBytesParser(data)
		reqHeader.Deserialize(parser)

		respHeader := &request.ResponseHeader{
			CorrelationId: reqHeader.CorrelationId,
		}

		respHeaderData, _ := respHeader.Serialize()

		switch reqHeader.ApiKey {
		case utils.ApiVersions:
			respBody, err := api.HandleApiVersionsRequest(reqHeader)
			if err != nil {
				fmt.Printf("Error handling ApiVersions request: %s\n", err.Error())
			}
			respBodyData, _ := respBody.Serialize()
			Send(c, append(respHeaderData, respBodyData...))

		case utils.DescribeTopicPartitions:
			respBody, err := api.HandleDescribeTopicPartitionsRequest(reqHeader, parser)
			if err != nil {
				fmt.Printf("Error handling DescribeTopicPartitions request: %s\n", err.Error())
				os.Exit(1)
			}
			respBodyData, _ := respBody.Serialize()
			Send(c, append(respHeaderData, respBodyData...))
		}
	}
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}
