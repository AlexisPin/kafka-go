package api

type FetchRequest struct {
	MaxWaitMs				int32
	MinBytes				int32
	MaxBytes				int32
	IsolationLevel			int8
	SessionId				int32
	SessionEpoch			int32
	Topics					[]FetchTopic
	ForgottenTopicsData		[]ForgottenTopicData
}

type FetchTopic struct {
	TopicId					string
	Partitions				[]FetchPartition
}

type ForgottenTopicData struct {
	TopicId					string
	Partitions				[]int32
}

type FetchPartition struct {
	PartitionId				int32
	CurrentLeaderEpoch		int32
	FetchOffset				int64
	LastFetchedEpoch		int32
	LogStartOffset			int64
	PartitionMaxBytes		int32
}
