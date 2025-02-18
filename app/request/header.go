package request

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
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
	res := make([]byte, 4)
	binary.BigEndian.PutUint32(res, uint32(r.CorrelationId))

	return res, nil
}

func (r *RequestHeader) Deserialize(p *decoder.BytesParser) error {
	r.ApiKey = utils.APIKeys(p.ReadInt16())
	r.ApiVersion = int16(p.ReadInt16())
	r.CorrelationId = int32(p.ReadInt32())
	r.ClientId = p.ReadNullableString()
	p.ReadInt8() // Tag Buffer
	return nil
}
