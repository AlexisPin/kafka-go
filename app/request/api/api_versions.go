package api

import (
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

type ApiVersionsRequest struct {
	ClientId              string
	ClientSoftwareVersion string
	TagBuffer             []byte
}

type ApiVersionsResponse struct {
	ErrorCode    utils.ErrorCode
	APIVersions  []APIVersions
	ThrottleTime int32
	TagBuffer    []byte
}

type APIVersions struct {
	ApiKey     int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  []byte
}

func (r *ApiVersionsRequest) Deserialize(c []byte) error {
	offset := 0
	clientIdLength := int(c[offset]) - 1
	offset += 1
	r.ClientId = string(c[offset : offset+clientIdLength])
	offset += clientIdLength

	clientSoftwareVersionLength := int(c[offset]) - 1
	offset += 1
	r.ClientSoftwareVersion = string(c[offset : offset+clientSoftwareVersionLength])
	offset += clientSoftwareVersionLength
	r.TagBuffer = c[offset:]
	return nil
}

func (r *APIVersions) Serialize() ([]byte, error) {
	res := make([]byte, 0)
	res = binary.BigEndian.AppendUint16(res, uint16(r.ApiKey))
	res = binary.BigEndian.AppendUint16(res, uint16(r.MinVersion))
	res = binary.BigEndian.AppendUint16(res, uint16(r.MaxVersion))
	res = append(res, r.TagBuffer...)
	return res, nil
}

func (r *ApiVersionsResponse) Serialize() ([]byte, error) {
	res := make([]byte, 0)
	res = binary.BigEndian.AppendUint16(res, uint16(r.ErrorCode))
	res = append(res, byte(len(r.APIVersions)+1))
	for _, apiVersion := range r.APIVersions {
		apiVersionBytes, _ := apiVersion.Serialize()
		res = append(res, apiVersionBytes...)
	}
	res = binary.BigEndian.AppendUint32(res, uint32(r.ThrottleTime))
	res = append(res, r.TagBuffer...)
	return res, nil
}

func HandleApiVersionsRequest(req *request.RequestHeader) (*ApiVersionsResponse, error) {
	if req.ApiVersion < 0 || req.ApiVersion > 4 {
		return &ApiVersionsResponse{
			ErrorCode: utils.UNSUPPORTED_VERSION,
		}, fmt.Errorf("unsupported version: %d", req.ApiVersion)
	}

	response := &ApiVersionsResponse{
		ErrorCode:    0,
		APIVersions:  []APIVersions{},
		ThrottleTime: 0,
		TagBuffer:    []byte{0},
	}
	response.APIVersions = append(response.APIVersions, APIVersions{
		ApiKey:     int16(utils.ApiVersions),
		MinVersion: 0,
		MaxVersion: 4,
		TagBuffer:  []byte{0},
	})
	response.APIVersions = append(response.APIVersions, APIVersions{
		ApiKey:     int16(utils.DescribeTopicPartitions),
		MinVersion: 0,
		MaxVersion: 0,
		TagBuffer:  []byte{0},
	})
	response.APIVersions = append(response.APIVersions, APIVersions{
		ApiKey:     int16(utils.Fetch),
		MinVersion: 0,
		MaxVersion: 16,
		TagBuffer:  []byte{0},
	})
	return response, nil
}
