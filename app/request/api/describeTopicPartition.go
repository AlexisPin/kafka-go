package api

import (
	"encoding/binary"

	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

type DescribeTopicPartitionsRequest struct {
	TopicNames             []string
	ResponsePartitionLimit int32
	Cursor                 Cursor
}

type DescribeTopicPartitionsResponse struct {
	ThrottleTime int32
	Topics       []Topic
	NextCursor   Cursor
}

type Cursor struct {
	TopicName      string
	Partitionindex int32
}

type TopicAuthorizedOperations int32

const (
	READ             TopicAuthorizedOperations = 1 << 3
	WRITE            TopicAuthorizedOperations = 1 << 4
	CREATE           TopicAuthorizedOperations = 1 << 5
	DELETE           TopicAuthorizedOperations = 1 << 6
	ALTER            TopicAuthorizedOperations = 1 << 7
	DESCRIBE         TopicAuthorizedOperations = 1 << 8
	DESCRIBE_CONFIGS TopicAuthorizedOperations = 1 << 10
	ALTER_CONFIGS    TopicAuthorizedOperations = 1 << 11
)

type Topic struct {
	ErrorCode                 utils.ErrorCode
	TopicName                 string
	TopicId                   string // A 16-byte UUID
	IsInternal                bool
	Partitions                []Partition
	TopicAuthorizedOperations TopicAuthorizedOperations
}

type Partition struct {
	ErrorCode utils.ErrorCode
	Partition int32
	Leader    int32
}

func (r *DescribeTopicPartitionsRequest) Deserialize(c []byte) error {
	offset := 0
	arrayLength := int(c[offset]) - 1
	offset += 1
	for range arrayLength {
		topicNameLength := int(c[offset]) - 1
		offset += 1
		r.TopicNames = append(r.TopicNames, string(c[offset:offset+topicNameLength]))
		offset += topicNameLength
	}
	offset += 1 // tag buffer
	r.ResponsePartitionLimit = int32(binary.BigEndian.Uint32(c[offset : offset+4]))
	offset += 1 // Cursor
	offset += 1 // tag buffer
	return nil
}

func (r *DescribeTopicPartitionsResponse) Serialize() ([]byte, error) {
	res := make([]byte, 0)
	res = binary.BigEndian.AppendUint32(res, uint32(r.ThrottleTime))
	res = append(res, byte(len(r.Topics)+1))
	for _, topic := range r.Topics {
		res = binary.BigEndian.AppendUint16(res, uint16(topic.ErrorCode))
		res = append(res, byte(len(topic.TopicName)+1))
		res = append(res, []byte(topic.TopicName)...)
		res = append(res, []byte(topic.TopicId)...)
		if topic.IsInternal {
			res = append(res, 1)
		} else {
			res = append(res, 0)
		}
		res = append(res, 1) // Empty Partitions Array
		res = binary.BigEndian.AppendUint32(res, uint32(topic.TopicAuthorizedOperations))
		res = append(res, 1) // Tagged Buffer
	}
	res = append(res, 0xff) // Next Cursor
	res = append(res, 0)    // Tagged Buffer
	return res, nil
}

func HandleDescribeTopicPartitionsRequest(req *request.RequestHeader, data []byte) (*DescribeTopicPartitionsResponse, error) {
	request := &DescribeTopicPartitionsRequest{}
	request.Deserialize(data[req.Size:])

	response := &DescribeTopicPartitionsResponse{
		ThrottleTime: 0,
		Topics:       []Topic{},
		NextCursor:   Cursor{},
	}
	for _, topicName := range request.TopicNames {
		response.Topics = append(response.Topics, Topic{
			ErrorCode: utils.UNKNOWN_TOPIC_OR_PARTITION,
			TopicName: topicName,
			TopicId: string([]byte{
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00,
				0x00, 0x00,
				0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			}),
			IsInternal:                false,
			Partitions:                []Partition{},
			TopicAuthorizedOperations: READ | WRITE | CREATE | DELETE | ALTER | DESCRIBE | DESCRIBE_CONFIGS | ALTER_CONFIGS,
		})
	}
	return response, nil
}
