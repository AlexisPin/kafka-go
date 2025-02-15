package api

import (
	"encoding/binary"
	"fmt"
)

type ApiVersionsRequest struct {
	ClientId string
	ClientSoftwareVersion string
}

type ApiVersionsResponse struct {
	ErrorCode ErrorCode
	APIVersions []APIVersions
	ThtottleTime int32
}

type APIVersions struct {
	ApiKey int16
	MinVersion int16
	MaxVersion int16
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

	return nil
}
