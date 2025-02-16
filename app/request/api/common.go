package api

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

type ResponseBody struct {
	ErrorCode utils.ErrorCode
}

func (r *ResponseBody) Serialize() ([]byte, error) {
	res := make([]byte, 2)
	binary.BigEndian.PutUint16(res, uint16(r.ErrorCode))

	return res, nil
}
