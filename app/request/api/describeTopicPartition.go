package api

import (
	"bytes"
	"encoding/binary"
	"os"

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

type MetatdataRecordType int8

const (
	TopicRecordType     MetatdataRecordType = 2
	PartitionRecordType MetatdataRecordType = 3
)

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
	ErrorCode                             utils.ErrorCode
	PartitionIndex                        int32
	LeaderId                              int32
	LeaderEpoch                           int32
	ReplicaNodeIds                        []int32
	IsrNodeIds                            []int32
	EligibleLeaderReplicaNodeIds          []int32
	LastKnownEligibleLeaderReplicaNodeIds []int32
	OfflineReplicaNodeIds                 []int32
	TaggedBuffer                          []byte
}

func ParseMetadataLogFile(path string) (map[string]Topic, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	buffer := bytes.NewBuffer(data)
	topics := map[string]*Topic{}

	for {
		// BaseOffset: (8 bytes)
		// BatchLength: (4 bytes)
		// PartitionLeaderEpoch: (4 bytes)
		// MagicByte: (1 byte)
		// CRC: (4 bytes)
		// Attribute: (2 bytes)
		// LastOffSetDelta: (4 bytes)
		// BaseTimestamp: (8 bytes)
		// MaxTimestamp: (8 bytes)
		// ProducerID: (8 bytes)
		// ProducerEpoch: (2 bytes)
		// BaseSequence: (4 bytes)
		_ = buffer.Next(8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4)
		if buffer.Len() == 0 {
			break
		}

		recordsLength := int32(binary.BigEndian.Uint32(buffer.Next(4)))
		for range recordsLength {
			length, _ := binary.ReadVarint(buffer)
			recordBuffer := bytes.NewBuffer(buffer.Next(int(length)))
			recordBuffer.Next(1)            // Attributes
			binary.ReadVarint(recordBuffer) // Timestamp Delta
			binary.ReadVarint(recordBuffer) // Offset Delta
			keyLength, _ := binary.ReadVarint(recordBuffer)
			if keyLength > 0 {
				recordBuffer.Next(int(keyLength))
			}
			valueLength, _ := binary.ReadVarint(recordBuffer)
			valueBuffer := bytes.NewBuffer(recordBuffer.Next(int(valueLength)))
			_ = valueBuffer.Next(1) // Frame Version
			var recordType MetatdataRecordType
			binary.Read(valueBuffer, binary.BigEndian, &recordType)
			valueBuffer.Next(1) // Version

			switch recordType {
			case TopicRecordType:
				nameLength, _ := binary.ReadUvarint(valueBuffer)
				topicName, topicId := make([]byte, nameLength), make([]byte, 16)
				binary.Read(valueBuffer, binary.BigEndian, &topicName)
				binary.Read(valueBuffer, binary.BigEndian, &topicId)

				topic := &Topic{
					TopicName: string(topicName),
					TopicId:   string(topicId),
				}
				topics[topic.TopicId] = topic

			case PartitionRecordType:
				partition := Partition{}
				binary.Read(valueBuffer, binary.BigEndian, &partition.PartitionIndex)
				topicId := make([]byte, 16)
				binary.Read(valueBuffer, binary.BigEndian, &topicId)

				replicaLength, _ := binary.ReadUvarint(valueBuffer)
				partition.ReplicaNodeIds = make([]int32, replicaLength-1)
				for i := range replicaLength - 1 {
					binary.Read(valueBuffer, binary.BigEndian, &partition.ReplicaNodeIds[i])
				}

				isrLength, _ := binary.ReadUvarint(valueBuffer)
				partition.IsrNodeIds = make([]int32, isrLength-1)
				for i := range isrLength - 1 {
					binary.Read(valueBuffer, binary.BigEndian, &partition.IsrNodeIds[i])
				}

				binary.ReadUvarint(valueBuffer) // Length of Removing Replicas array
				binary.ReadUvarint(valueBuffer) // Length of Adding Replicas array
				binary.Read(valueBuffer, binary.BigEndian, &partition.LeaderId)
				binary.Read(valueBuffer, binary.BigEndian, &partition.LeaderEpoch)

				topics[string(topicId)].Partitions = append(topics[string(topicId)].Partitions, partition)
			}
		}
	}
	topicsList := make(map[string]Topic, len(topics))
	for _, topic := range topics {
		topicsList[topic.TopicName] = *topic
	}

	return topicsList, nil

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
		offset += 1 // tag buffer
	}
	r.ResponsePartitionLimit = int32(binary.BigEndian.Uint32(c[offset : offset+4]))
	offset += 4
	cursorLen := int16(binary.BigEndian.Uint16(c[offset : offset+2]))
	if cursorLen > 0 {
		offset += 2
		r.Cursor.TopicName = string(c[offset : offset+int(cursorLen)])
		offset += int(cursorLen)
		r.Cursor.Partitionindex = int32(binary.BigEndian.Uint32(c[offset : offset+4]))
		offset += 4
	}
	offset += 1 // tag buffer
	return nil
}

func (r *DescribeTopicPartitionsResponse) Serialize() ([]byte, error) {
	res := make([]byte, 0)
	res = append(res, 0) // Tag Buffer Response Header v1
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
		res = append(res, byte(len(topic.Partitions)+1))
		for _, partition := range topic.Partitions {
			res = binary.BigEndian.AppendUint16(res, uint16(partition.ErrorCode))
			res = binary.BigEndian.AppendUint32(res, uint32(partition.PartitionIndex))
			res = binary.BigEndian.AppendUint32(res, uint32(partition.LeaderId))
			res = binary.BigEndian.AppendUint32(res, uint32(partition.LeaderEpoch))
			res = append(res, byte(len(partition.ReplicaNodeIds)+1))
			for _, replicaNodeId := range partition.ReplicaNodeIds {
				res = binary.BigEndian.AppendUint32(res, uint32(replicaNodeId))
			}
			res = append(res, byte(len(partition.IsrNodeIds)+1))
			for _, isrNodeId := range partition.IsrNodeIds {
				res = binary.BigEndian.AppendUint32(res, uint32(isrNodeId))
			}
			res = append(res, byte(len(partition.EligibleLeaderReplicaNodeIds)+1))
			for _, eligibleLeaderReplicaNodeId := range partition.EligibleLeaderReplicaNodeIds {
				res = binary.BigEndian.AppendUint32(res, uint32(eligibleLeaderReplicaNodeId))
			}
			res = append(res, byte(len(partition.LastKnownEligibleLeaderReplicaNodeIds)+1))
			for _, lastKnownEligibleLeaderReplicaNodeId := range partition.LastKnownEligibleLeaderReplicaNodeIds {
				res = binary.BigEndian.AppendUint32(res, uint32(lastKnownEligibleLeaderReplicaNodeId))
			}
			res = append(res, byte(len(partition.OfflineReplicaNodeIds)+1))
			for _, offlineReplicaNodeId := range partition.OfflineReplicaNodeIds {
				res = binary.BigEndian.AppendUint32(res, uint32(offlineReplicaNodeId))
			}
			res = append(res, 0) // Tagged Buffer
		}
		res = binary.BigEndian.AppendUint32(res, uint32(topic.TopicAuthorizedOperations))
		res = append(res, 0) // Tagged Buffer
	}
	res = append(res, 0xff) // Next Cursor
	res = append(res, 0x00) // Tagged Buffer
	return res, nil
}

func HandleDescribeTopicPartitionsRequest(req *request.RequestHeader, data []byte) (*DescribeTopicPartitionsResponse, error) {
	request := &DescribeTopicPartitionsRequest{}
	request.Deserialize(data[req.Size:])

	response := &DescribeTopicPartitionsResponse{
		ThrottleTime: 0,
		Topics:       []Topic{},
		NextCursor: Cursor{
			TopicName:      request.Cursor.TopicName,
			Partitionindex: request.Cursor.Partitionindex,
		},
	}
	topics, err := ParseMetadataLogFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return nil, err
	}
	for _, topicName := range request.TopicNames {
		curTopic, ok := topics[topicName]
		if !ok {
			continue
		}
		response.Topics = append(response.Topics, Topic{
			ErrorCode:                 utils.NONE,
			TopicName:                 curTopic.TopicName,
			TopicId:                   curTopic.TopicId,
			IsInternal:                false,
			Partitions:                curTopic.Partitions,
			TopicAuthorizedOperations: READ | WRITE | CREATE | DELETE | ALTER | DESCRIBE | DESCRIBE_CONFIGS | ALTER_CONFIGS,
		})
	}
	return response, nil
}
