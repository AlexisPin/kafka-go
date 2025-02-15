package api

import "encoding/binary"

type ResponseBody struct {
	ErrorCode ErrorCode
}

func (r *ResponseBody) Serialize() ([]byte, error) {
	res := make([]byte, 2)
	binary.BigEndian.PutUint16(res, uint16(r.ErrorCode))

	return res, nil
}
