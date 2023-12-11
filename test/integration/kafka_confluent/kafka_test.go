/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_confluent

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	kafka_confluent "github.com/cloudevents/sdk-go/protocol/kafka_confluent/v2"
	clienttest "github.com/cloudevents/sdk-go/v2/client/test"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/test"
)

const (
	TEST_GROUP_ID = "test_confluent_group_id"
)

var (
	TopicName = "test-ce-confluent-" + uuid.New().String()
	ctx       = context.Background()
)

func TestSendEvent(t *testing.T) {
	test.EachEvent(t, test.Events(), func(t *testing.T, eventIn event.Event) {
		eventIn = test.ConvertEventExtensionsToString(t, eventIn)
		clienttest.SendReceive(t, func() interface{} {
			protocol, err := protocolFactory(t)
			require.NoError(t, err)
			return protocol
		}, eventIn, func(eventOut event.Event) {
			test.AllOf(
				test.HasExactlyAttributesEqualTo(eventIn.Context),
				test.HasData(eventIn.Data()),
				test.HasExtensionKeys([]string{kafka_confluent.KafkaPartitionKey, kafka_confluent.KafkaOffsetKey}),
				test.HasExtension(kafka_confluent.KafkaTopicKey, TopicName),
			)
		})
	})
}

// To start a local environment for testing:
// docker run --rm --net=host -e ADV_HOST=localhost -e SAMPLEDATA=0 -p 9092:9092 lensesio/fast-data-dev
func protocolFactory(t testing.TB) (*kafka_confluent.Protocol, error) {
	bootstrapSever := "localhost:9092"

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapSever,
		"group.id":           TEST_GROUP_ID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		return nil, err
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapSever})
	if err != nil {
		return nil, err
	}

	return kafka_confluent.New(ctx, nil,
		kafka_confluent.WithReceiver(consumer),
		kafka_confluent.WithReceiverTopics([]string{TopicName}),
		kafka_confluent.WithSender(producer))
}
