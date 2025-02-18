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
	TopicId            string
	PartitionResponses []FetchPartitionResponse
}

type FetchPartitionResponse struct {
	PartitionIndex   int32
	ErrorCode        utils.ErrorCode
	HighWatermark    int64
	LastStableOffset int64
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
	r.Partitions = make([]FetchPartition, p.ReadInt32()-1)
	for range r.Partitions {
		parts := FetchPartition{}
		parts.Deserialize(p)
		r.Partitions = append(r.Partitions, parts)
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
	r.Topics = make([]FetchTopic, p.ReadInt32()-1)
	for range r.Topics {
		topic := FetchTopic{}
		topic.Deserialize(p)
		r.Topics = append(r.Topics, topic)
	}
	r.ForgottenTopicsData = make([]ForgottenTopicData, p.ReadInt32()-1)
	for i := range r.ForgottenTopicsData {
		r.ForgottenTopicsData[i].TopicId = string(p.ReadUUID())
		r.ForgottenTopicsData[i].Partitions = make([]int32, p.ReadInt32()-1)
		for j := range r.ForgottenTopicsData[i].Partitions {
			r.ForgottenTopicsData[i].Partitions[j] = p.ReadInt32()
		}
		p.ReadInt8() // Tag Buffer
	}
	return nil
}

func (r *FetchResponse) Serialize() ([]byte, error) {
	b := new(bytes.Buffer)
	binary.Write(b, binary.BigEndian, r.ThrottleTimeMs)
	binary.Write(b, binary.BigEndian, r.ErrorCode)
	binary.Write(b, binary.BigEndian, r.SessionId)

	// Responses
	binary.Write(b, binary.BigEndian, int8(len(r.Responses)+1))
	for i := range r.Responses {
		binary.Write(b, binary.BigEndian, []byte(r.Responses[i].TopicId))

		binary.Write(b, binary.BigEndian, int8(len(r.Responses[i].PartitionResponses)+1))
		for j := range r.Responses[i].PartitionResponses {
			binary.Write(b, binary.BigEndian, r.Responses[i].PartitionResponses[j].PartitionIndex)
			binary.Write(b, binary.BigEndian, r.Responses[i].PartitionResponses[j].ErrorCode)
			binary.Write(b, binary.BigEndian, r.Responses[i].PartitionResponses[j].HighWatermark)
			binary.Write(b, binary.BigEndian, r.Responses[i].PartitionResponses[j].LastStableOffset)
		}
	}
	binary.Write(b, binary.BigEndian, int8(0)) // Tag Buffer
	return b.Bytes(), nil
}

func HandleFetchRequest(header *request.RequestHeader, p *decoder.BytesParser) (*FetchResponse, error) {
	req := &FetchRequest{}
	req.Deserialize(p)

	fmt.Printf("Handling Fetch request: %+v\n", req)

	resp := &FetchResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      utils.NONE,
		SessionId:      req.SessionId,
		Responses:      make([]FetchResponseTopic, len(req.Topics)),
	}

	for i := range req.Topics {
		resp.Responses[i].TopicId = req.Topics[i].TopicId
		resp.Responses[i].PartitionResponses = make([]FetchPartitionResponse, len(req.Topics[i].Partitions))
		for j := range req.Topics[i].Partitions {
			resp.Responses[i].PartitionResponses[j] = FetchPartitionResponse{
				PartitionIndex:   req.Topics[i].Partitions[j].PartitionId,
				ErrorCode:        utils.NONE,
				HighWatermark:    0,
				LastStableOffset: 0,
			}
		}
	}

	fmt.Printf("Sending Fetch response: %+v\n", resp)

	return resp, nil
}
