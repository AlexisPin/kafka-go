package api

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
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

func (r *Cursor) Serialize() ([]byte, error) {
	b := new(bytes.Buffer)
	topic := []byte(r.TopicName)
	binary.Write(b, binary.BigEndian, int8(len(topic)+1))
	b.Write(topic)
	binary.Write(b, binary.BigEndian, r.Partitionindex)
	binary.Write(b, binary.BigEndian, int8(0)) // Tag Buffer
	return b.Bytes(), nil
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

func (t *Topic) Serialize() ([]byte, error) {
	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, t.ErrorCode)
	if t.TopicName != "" {
		name := []byte(t.TopicName)
		binary.Write(b, binary.BigEndian, int8(len(name)+1))
		b.Write(name)
	} else {
		binary.Write(b, binary.BigEndian, int8(0))
	}
	binary.Write(b, binary.BigEndian, []byte(t.TopicId))
	if t.IsInternal {
		binary.Write(b, binary.BigEndian, int8(1))
	} else {
		binary.Write(b, binary.BigEndian, int8(0))
	}
	binary.Write(b, binary.BigEndian, int8(len(t.Partitions)+1))
	for _, partition := range t.Partitions {
		partitionBytes, _ := partition.Serialize()
		b.Write(partitionBytes)
	}
	binary.Write(b, binary.BigEndian, int32(t.TopicAuthorizedOperations))
	binary.Write(b, binary.BigEndian, int8(0)) // Tag Buffer
	return b.Bytes(), nil
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

func ParseMetadataLogFile() (map[string]Topic, error) {
	buffer, err := utils.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		fmt.Printf("Error reading metadata log file: %s\n", err.Error())
	}

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
		buffer.Next(8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4)
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
				topicName, topicId := make([]byte, nameLength-1), make([]byte, 16)
				binary.Read(valueBuffer, binary.BigEndian, &topicName)
				binary.Read(valueBuffer, binary.BigEndian, &topicId)

				topic := &Topic{
					TopicName: string(topicName),
					TopicId:   string(topicId),
				}

				topics[topic.TopicId] = topic

			case PartitionRecordType:
				partition := Partition{
					ErrorCode:                             utils.NONE,
					EligibleLeaderReplicaNodeIds:          []int32{},
					LastKnownEligibleLeaderReplicaNodeIds: []int32{},
					OfflineReplicaNodeIds:                 []int32{},
				}
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

func (r *DescribeTopicPartitionsRequest) Deserialize(p *decoder.BytesParser) error {
	arrayLength := p.ReadInt8() - 1

	for range arrayLength {
		r.TopicNames = append(r.TopicNames, p.ReadCompactString())
		p.ReadInt8() // tag buffer
	}
	r.ResponsePartitionLimit = p.ReadInt32()
	r.Cursor.TopicName = p.ReadCompactString()
	if r.Cursor.TopicName != "" {
		r.Cursor.Partitionindex = p.ReadInt32()
	}
	p.ReadInt8() // tag buffer
	return nil
}

func (r *Partition) Serialize() ([]byte, error) {
	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, r.ErrorCode)
	binary.Write(b, binary.BigEndian, r.PartitionIndex)
	binary.Write(b, binary.BigEndian, r.LeaderId)
	binary.Write(b, binary.BigEndian, r.LeaderEpoch)

	// Replica Nodes
	binary.Write(b, binary.BigEndian, int8(len(r.ReplicaNodeIds)+1))
	for _, replicaNodeId := range r.ReplicaNodeIds {
		binary.Write(b, binary.BigEndian, replicaNodeId)
	}

	// ISR Nodes
	binary.Write(b, binary.BigEndian, int8(len(r.IsrNodeIds)+1))
	for _, isrNodeId := range r.IsrNodeIds {
		binary.Write(b, binary.BigEndian, isrNodeId)
	}

	// Eligible Leader Replicas
	binary.Write(b, binary.BigEndian, int8(len(r.EligibleLeaderReplicaNodeIds)+1))
	for _, eligibleLeaderReplicaNodeId := range r.EligibleLeaderReplicaNodeIds {
		binary.Write(b, binary.BigEndian, eligibleLeaderReplicaNodeId)
	}

	// Last Known ELR
	binary.Write(b, binary.BigEndian, int8(len(r.LastKnownEligibleLeaderReplicaNodeIds)+1))
	for _, lk := range r.LastKnownEligibleLeaderReplicaNodeIds {
		binary.Write(b, binary.BigEndian, lk)
	}

	//  Offline Replicas
	binary.Write(b, binary.BigEndian, int8(len(r.OfflineReplicaNodeIds)+1))
	for _, offlineReplicaNodeId := range r.OfflineReplicaNodeIds {
		binary.Write(b, binary.BigEndian, offlineReplicaNodeId)
	}

	binary.Write(b, binary.BigEndian, int8(0)) // Tag Buffer
	return b.Bytes(), nil
}

func (r *DescribeTopicPartitionsResponse) Serialize() ([]byte, error) {
	b := new(bytes.Buffer)
	b.Write([]byte{0}) // Tag Buffer
	binary.Write(b, binary.BigEndian, r.ThrottleTime)
	binary.Write(b, binary.BigEndian, int8(len(r.Topics)+1))
	for _, topic := range r.Topics {
		topicBytes, _ := topic.Serialize()
		b.Write(topicBytes)
	}
	b.Write([]byte{0xff})                      // Next Cursor
	binary.Write(b, binary.BigEndian, int8(0)) // Tag Buffer
	return b.Bytes(), nil
}

func HandleDescribeTopicPartitionsRequest(req *request.RequestHeader, p *decoder.BytesParser) (*DescribeTopicPartitionsResponse, error) {
	request := &DescribeTopicPartitionsRequest{}
	request.Deserialize(p)

	response := &DescribeTopicPartitionsResponse{
		ThrottleTime: 0,
		Topics:       []Topic{},
		NextCursor: Cursor{
			TopicName:      request.Cursor.TopicName,
			Partitionindex: request.Cursor.Partitionindex,
		},
	}
	topics, err := ParseMetadataLogFile()
	if err != nil {
		return nil, err
	}
	for _, topicName := range request.TopicNames {
		curTopic, ok := topics[topicName]
		errorCode := utils.UNKNOWN_TOPIC_OR_PARTITION
		partition := []Partition{}
		topicId := string([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

		if ok {
			partition = curTopic.Partitions
			errorCode = utils.NONE
			topicId = curTopic.TopicId
		}

		response.Topics = append(response.Topics, Topic{
			ErrorCode:                 errorCode,
			TopicName:                 topicName,
			TopicId:                   topicId,
			IsInternal:                false,
			Partitions:                partition,
			TopicAuthorizedOperations: READ | WRITE | CREATE | DELETE | ALTER | DESCRIBE | DESCRIBE_CONFIGS | ALTER_CONFIGS,
		})
	}
	return response, nil
}
