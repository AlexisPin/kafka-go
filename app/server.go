package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

type APIKeys int16
type ErrorCode int16

const (
	ApiVersions APIKeys = 18
)

const (
	NONE                ErrorCode = 0
	UNSUPPORTED_VERSION ErrorCode = 35
)

type Request struct {
	CorrelationId int32
	ErrorCode     ErrorCode
	ApiKey        APIKeys
}

func Send(c net.Conn, b []byte) error {
	binary.Write(c, binary.BigEndian, int32(len(b)))
	binary.Write(c, binary.BigEndian, b)

	return nil
}

func Read(c net.Conn) (*Request, error) {
	var MessageSize int32
	var ApiVersion int16

	req := Request{}

	binary.Read(c, binary.BigEndian, &MessageSize)
	binary.Read(c, binary.BigEndian, &req.ApiKey)
	binary.Read(c, binary.BigEndian, &ApiVersion)
	binary.Read(c, binary.BigEndian, &req.CorrelationId)

	if req.ApiKey != ApiVersions {
		return nil, fmt.Errorf("invalid API key %d", req.ApiKey)
	}

	if ApiVersion < 0 || ApiVersion > 4 {
		req.ErrorCode = UNSUPPORTED_VERSION
	} else {
		req.ErrorCode = NONE
	}

	message := make([]byte, MessageSize-8)
	c.Read(message)

	return &req, nil
}

func handleConnection(c net.Conn) {
	defer c.Close()

	for {
		parsed_req, err := Read(c)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			os.Exit(1)
		}

		if parsed_req.ErrorCode != 0 {
			response := make([]byte, 6)
			binary.BigEndian.PutUint32(response[:4], uint32(parsed_req.CorrelationId))
			binary.BigEndian.PutUint16(response[4:], uint16(parsed_req.ErrorCode))
			Send(c, response)
			os.Exit(1)
		}

		response := make([]byte, 19)
		binary.BigEndian.PutUint32(response[:4], uint32(parsed_req.CorrelationId)) // Correlation ID
		binary.BigEndian.PutUint16(response[4:6], uint16(parsed_req.ErrorCode))    // Error Code
		response[6] = 2                                                            // Number of API keys
		binary.BigEndian.PutUint16(response[7:9], uint16(parsed_req.ApiKey))       // API Key
		binary.BigEndian.PutUint16(response[9:11], 0)                              // Min Version
		binary.BigEndian.PutUint16(response[11:13], 4)                             // Max Version
		response[13] = 0                                                           // Number of Tagged Fields
		binary.BigEndian.PutUint32(response[14:18], 0)                             // Throttle Time
		response[18] = 0                                                           // Number of Tagged Fields

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
