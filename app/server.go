package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/request/api"
)

func Send(c net.Conn, b []byte) error {
	binary.Write(c, binary.BigEndian, int32(len(b)))
	binary.Write(c, binary.BigEndian, b)

	return nil
}

func handleConnection(c net.Conn) {
	defer c.Close()

	for {
		var message_size uint32
		err := binary.Read(c, binary.BigEndian, &message_size)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		data := make([]byte, message_size)
		_, err = c.Read(data)

		reqHeader := &api.RequestHeader{}
		reqHeader.Deserialize(data)

		respHeader := api.ResponseHeader{}
		respHeader.CorrelationId = reqHeader.CorrelationId
		rspHeaderData, _ := respHeader.Serialize()

		switch reqHeader.ApiKey {
		case api.ApiVersions:
		}

		response := make([]byte, 26)
		binary.BigEndian.PutUint32(response[:4], uint32(parsed_req.CorrelationId))   // Correlation ID
		binary.BigEndian.PutUint16(response[4:6], uint16(parsed_req.ErrorCode))      // Error Code
		response[6] = 3                                                              // Number of API keys
		binary.BigEndian.PutUint16(response[7:9], uint16(parsed_req.ApiKey))         // API Key
		binary.BigEndian.PutUint16(response[9:11], 0)                                // Min Version
		binary.BigEndian.PutUint16(response[11:13], 4)                               // Max Version
		response[13] = 0                                                             // Number of Tagged Fields
		binary.BigEndian.PutUint16(response[14:16], uint16(DescribeTopicPartitions)) // API Key
		binary.BigEndian.PutUint16(response[16:18], 0)                               // Min Version
		binary.BigEndian.PutUint16(response[18:20], 0)                               // Max Version
		response[20] = 0                                                             // Number of Tagged Fields
		binary.BigEndian.PutUint32(response[21:25], 0)                               // Throttle Time
		response[25] = 0                                                             // tagged fields

		Send(c, response)
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
