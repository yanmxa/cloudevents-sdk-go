module github.com/cloudevents/sdk-go/protocol/kafka_confluent/v2

go 1.18

replace github.com/cloudevents/sdk-go/v2 => ../../../v2

require (
	github.com/cloudevents/sdk-go/v2 v2.0.0-00010101000000-000000000000
	github.com/confluentinc/confluent-kafka-go v1.9.2
)

require (
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
)
