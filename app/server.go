package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

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

	buff := make([]byte, 1024)
	data_len, err := conn.Read(buff)
	if err != nil {
		fmt.Println("Error reading data: ", err.Error())
		os.Exit(1)
	}

	if data_len == 0 {
		fmt.Println("No data received")
		os.Exit(1)
	}

	var message_size = buff[0:4]
	// var request_api_key = buff[4:6]
	// var request_api_version = buff[6:8]
	var correlation_id = buff[8:12]

	response := make([]byte, 10)
	copy(response[:4], message_size)
	copy(response[4:8], correlation_id)
	copy(response[8:], []byte{0, 35})

	conn.Write(response)
}
