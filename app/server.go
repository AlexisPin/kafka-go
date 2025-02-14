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

type Message struct {
	correlation_id int32
	error_code     ErrorCode
	api_key        APIKeys
}

func Send(c net.Conn, b []byte) error {
	binary.Write(c, binary.BigEndian, int32(len(b)))
	binary.Write(c, binary.BigEndian, b)

	return nil
}

func Read(c net.Conn) (*Message, error) {
	var message_size, correlation_id int32
	var api_key APIKeys
	var api_version int16

	data := Message{}

	binary.Read(c, binary.BigEndian, &message_size)
	binary.Read(c, binary.BigEndian, &api_key)
	binary.Read(c, binary.BigEndian, &api_version)
	binary.Read(c, binary.BigEndian, &correlation_id)

	data.correlation_id = correlation_id
	data.api_key = api_key

	if api_key != ApiVersions {
		return nil, fmt.Errorf("invalid API key")
	}

	if api_version < 0 || api_version > 4 {
		data.error_code = UNSUPPORTED_VERSION
	} else {
		data.error_code = NONE
	}

	message := make([]byte, message_size-8)
	c.Read(message)

	return &data, nil
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	parsed_buffer, err := Read(conn)
	if err != nil {
		fmt.Println("Error reading from connection: ", err.Error())
		os.Exit(1)
	}

	if parsed_buffer.error_code != 0 {
		response := make([]byte, 6)
		binary.BigEndian.PutUint32(response[:4], uint32(parsed_buffer.correlation_id))
		binary.BigEndian.PutUint16(response[4:], uint16(parsed_buffer.error_code))
		Send(conn, response)
		os.Exit(1)
	}

	// ApiVersions Response (Version: 4) => error_code [api_keys] throttle_time_ms TAG_BUFFER 
  // error_code => INT16
  // api_keys => api_key min_version max_version TAG_BUFFER 
  //   api_key => INT16
  //   min_version => INT16
  //   max_version => INT16
	// _tagged_fields
	// throttle_time_ms => INT32
	// _tagged_fields

	response := make([]byte, 19)
	binary.BigEndian.PutUint32(response[:4], uint32(parsed_buffer.correlation_id)) // Correlation ID
	binary.BigEndian.PutUint16(response[4:6], uint16(parsed_buffer.error_code)) 	// Error Code
	response[6] = 2 // Number of API keys
	binary.BigEndian.PutUint16(response[7:9], uint16(parsed_buffer.api_key)) // API Key
	binary.BigEndian.PutUint16(response[9:11], 0) // Min Version
	binary.BigEndian.PutUint16(response[11:13], 4) // Max Version
	response[13] = 0 // Number of Tagged Fields
	binary.BigEndian.PutUint32(response[14:18], 0) // Throttle Time
	response[18] = 0 // Number of Tagged Fields

	Send(conn, response)
}
