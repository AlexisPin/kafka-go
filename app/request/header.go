package request

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

type RequestHeader struct {
	Size          uint
	ApiKey        utils.APIKeys
	ApiVersion    int16
	CorrelationId int32
	ClientId      string
}

type ResponseHeader struct {
	CorrelationId int32
}

func (r *ResponseHeader) Serialize() ([]byte, error) {
	res := make([]byte, 6)
	binary.BigEndian.PutUint32(res[:4], uint32(r.CorrelationId))

	return res, nil
}

func (r *RequestHeader) Deserialize(b []byte) error {
	r.ApiKey = utils.APIKeys(binary.BigEndian.Uint16(b[:2]))
	r.ApiVersion = int16(binary.BigEndian.Uint16(b[2:4]))
	r.CorrelationId = int32(binary.BigEndian.Uint32(b[4:8]))
	clientIdLength := uint16(binary.BigEndian.Uint16(b[8:10]))
	if clientIdLength > 0 {
		r.ClientId = string(b[10 : 10+clientIdLength])
	}
	size := 10 + clientIdLength
	size += 1 // tag buffer
	r.Size = uint(size)
	return nil
}
