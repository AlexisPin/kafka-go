package utils

type APIKeys int16
type ErrorCode int16

const (
	ApiVersions             APIKeys = 18
	DescribeTopicPartitions APIKeys = 75
)

const (
	NONE                       ErrorCode = 0
	UNSUPPORTED_VERSION        ErrorCode = 35
	UNKNOWN_TOPIC_OR_PARTITION ErrorCode = 3
)
