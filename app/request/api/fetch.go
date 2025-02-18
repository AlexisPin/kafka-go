package api

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/codecrafters-io/kafka-starter-go/app/decoder"
	"github.com/codecrafters-io/kafka-starter-go/app/request"
	"github.com/codecrafters-io/kafka-starter-go/app/utils"
)

type FetchRequest struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionId           int32
	SessionEpoch        int32
	Topics              []FetchTopic
	ForgottenTopicsData []ForgottenTopicData
	RackId              string
}

type FetchTopic struct {
	TopicId    string
	Partitions []FetchPartition
}

type ForgottenTopicData struct {
	TopicId    string
	Partitions []int32
}

type FetchPartition struct {
	PartitionId        int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}

type FetchResponse struct {
	ThrottleTimeMs int32
	ErrorCode      utils.ErrorCode
	SessionId      int32
	Responses      []FetchResponseTopic
}

type FetchResponseTopic struct {
	TopicId    string
	Partitions []FetchPartitionResponse
}

type FetchPartitionResponse struct {
	PartitionIndex       int32
	ErrorCode            utils.ErrorCode
	HighWatermark        int64
	LastStableOffset     int64
	LogStartOffset       int64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica int32
	Records              []byte
}

type AbortedTransaction struct {
	ProducerId  int64
	FirstOffset int64
}

func (r *FetchPartition) Deserialize(p *decoder.BytesParser) error {
	r.PartitionId = p.ReadInt32()
	r.CurrentLeaderEpoch = p.ReadInt32()
	r.FetchOffset = p.ReadInt64()
	r.LastFetchedEpoch = p.ReadInt32()
	r.LogStartOffset = p.ReadInt64()
	r.PartitionMaxBytes = p.ReadInt32()
	p.ReadInt8() // Tag Buffer
	return nil
}

func (r *FetchTopic) Deserialize(p *decoder.BytesParser) error {
	r.TopicId = string(p.ReadUUID())
	r.Partitions = make([]FetchPartition, p.ReadInt8()-1)
	for i := range r.Partitions {
		parts := FetchPartition{}
		parts.Deserialize(p)
		r.Partitions[i] = parts
		p.ReadInt8() // Tag Buffer
	}
	return nil
}

func (r *FetchRequest) Deserialize(p *decoder.BytesParser) error {
	r.MaxWaitMs = p.ReadInt32()
	r.MinBytes = p.ReadInt32()
	r.MaxBytes = p.ReadInt32()
	r.IsolationLevel = p.ReadInt8()
	r.SessionId = p.ReadInt32()
	r.SessionEpoch = p.ReadInt32()
	r.Topics = make([]FetchTopic, p.ReadInt8()-1)

	for i := range r.Topics {
		topic := FetchTopic{}
		topic.Deserialize(p)
		r.Topics[i] = topic
	}

	r.ForgottenTopicsData = make([]ForgottenTopicData, p.ReadInt8()-1)
	for i := range r.ForgottenTopicsData {
		r.ForgottenTopicsData[i].TopicId = string(p.ReadUUID())
		r.ForgottenTopicsData[i].Partitions = make([]int32, p.ReadInt8())
		for j := range r.ForgottenTopicsData[i].Partitions {
			r.ForgottenTopicsData[i].Partitions[j] = p.ReadInt32()
		}
		p.ReadInt8() // Tag Buffer
	}
	r.RackId = p.ReadCompactString()
	p.ReadInt8() // Tag Buffer
	return nil
}

func (r *FetchResponse) Serialize() ([]byte, error) {
	b := new(bytes.Buffer)
	b.Write([]byte{0}) // Tag Buffer
	binary.Write(b, binary.BigEndian, r.ThrottleTimeMs)
	binary.Write(b, binary.BigEndian, r.ErrorCode)
	binary.Write(b, binary.BigEndian, r.SessionId)

	// Responses
	binary.Write(b, binary.BigEndian, int8(len(r.Responses)+1))
	for _, response := range r.Responses {
		binary.Write(b, binary.BigEndian, []byte(response.TopicId))
		binary.Write(b, binary.BigEndian, int8(len(response.Partitions)+1))

		// Partitions
		for _, partition := range response.Partitions {
			binary.Write(b, binary.BigEndian, partition.PartitionIndex)
			binary.Write(b, binary.BigEndian, partition.ErrorCode)
			binary.Write(b, binary.BigEndian, partition.HighWatermark)
			binary.Write(b, binary.BigEndian, partition.LastStableOffset)
			binary.Write(b, binary.BigEndian, partition.LogStartOffset)
			binary.Write(b, binary.BigEndian, int8(len(partition.AbortedTransactions)+1))

			for _, at := range partition.AbortedTransactions {
				binary.Write(b, binary.BigEndian, at.ProducerId)
				binary.Write(b, binary.BigEndian, at.FirstOffset)
				b.Write([]byte{0}) // Tag Buffer
			}
			binary.Write(b, binary.BigEndian, partition.PreferredReadReplica)
			binary.Write(b, binary.BigEndian, int8(len(partition.Records)+1))
			b.Write(partition.Records)
			b.Write([]byte{0}) // Tag Buffer
		}
		b.Write([]byte{0}) // Tag Buffer
	}
	b.Write([]byte{0}) // Tag Buffer
	return b.Bytes(), nil
}

var metadataTopics, _ = ParseMetadataLogFile()

func HandleFetchRequest(header *request.RequestHeader, p *decoder.BytesParser) (*FetchResponse, error) {
	req := &FetchRequest{}
	req.Deserialize(p)

	isTopicValid := map[string]string{}

	resp := &FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      utils.NONE,
		SessionId:      req.SessionId,
		Responses:      make([]FetchResponseTopic, len(req.Topics)),
	}

	for i, topic := range req.Topics {
		for topicName, metadataTopic := range metadataTopics {
			if topic.TopicId == metadataTopic.TopicId {
				isTopicValid[topic.TopicId] = topicName
			}
		}

		resp.Responses[i].TopicId = topic.TopicId
		resp.Responses[i].Partitions = make([]FetchPartitionResponse, len(topic.Partitions))

		errorCode := utils.NONE
		records := []byte{}
		for j, partition := range topic.Partitions {
			if isTopicValid[topic.TopicId] == "" {
				errorCode = utils.UNKNOWN_TOPIC_ID
			} else {
				buffer, err := ReadLogFile(isTopicValid[topic.TopicId], partition.PartitionId)
				if err != nil {
					return nil, err
				}
				records = buffer
			}
			resp.Responses[i].Partitions[j] = FetchPartitionResponse{
				PartitionIndex:       partition.PartitionId,
				ErrorCode:            errorCode,
				HighWatermark:        0,
				LastStableOffset:     0,
				LogStartOffset:       0,
				AbortedTransactions:  []AbortedTransaction{},
				PreferredReadReplica: 0,
				Records:              records,
			}
		}
	}
	return resp, nil
}

func ReadLogFile(topicName string, partitionId int32) ([]byte, error) {
	filePath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partitionId)
	buffer, err := utils.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
