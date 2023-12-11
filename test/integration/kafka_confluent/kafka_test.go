/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_confluent

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	kafka_confluent "github.com/cloudevents/sdk-go/protocol/kafka_confluent/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/test"
)

const (
	TEST_GROUP_ID = "test_confluent_group_id"
)

type receiveEvent struct {
	event cloudevents.Event
	err   error
}

func TestSendEvent(t *testing.T) {
	test.EachEvent(t, test.Events(), func(t *testing.T, eventIn event.Event) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		topicName := "test-ce-confluent-" + uuid.New().String()
		eventIn = test.ConvertEventExtensionsToString(t, eventIn)

		// start a cloudevents receiver client go to receive the event
		eventChan := make(chan receiveEvent)

		receiverReady := make(chan bool)
		go func() {
			p, err := protocolFactory(t, ctx, "", []string{topicName})
			if err != nil {
				eventChan <- receiveEvent{err: err}
				return
			}
			defer p.Close(ctx)

			client, err := cloudevents.NewClient(p)

			receiverReady <- true
			err = client.StartReceiver(ctx, func(event cloudevents.Event) {
				eventChan <- receiveEvent{event: event}
			})
			if err != nil {
				eventChan <- receiveEvent{err: err}
			}
		}()

		<-receiverReady

		// start a cloudevents sender client go to send the event
		p, err := protocolFactory(t, ctx, topicName, nil)
		require.NoError(t, err)
		client, err := cloudevents.NewClient(p)
		require.NoError(t, err)
		res := client.Send(ctx, eventIn)
		require.NoError(t, res)

		// check the received event
		receivedEvent := <-eventChan
		require.NoError(t, receivedEvent.err)
		eventOut := test.ConvertEventExtensionsToString(t, receivedEvent.event)

		// test.AssertEventEquals(t, eventIn, receivedEvent.event)
		err = test.AllOf(
			test.HasExactlyAttributesEqualTo(eventIn.Context),
			test.HasData(eventIn.Data()),
			test.HasExtensionKeys([]string{kafka_confluent.KafkaPartitionKey, kafka_confluent.KafkaOffsetKey}),
			test.HasExtension(kafka_confluent.KafkaTopicKey, topicName),
		)(eventOut)
		require.NoError(t, err)
	})
}

// To start a local environment for testing:
// docker run --rm --net=host -e ADV_HOST=localhost -e SAMPLEDATA=0 -p 9091:9091 -p 9092:9092 lensesio/fast-data-dev
func protocolFactory(t testing.TB, ctx context.Context, sendTopic string, receiveTopic []string,
) (*kafka_confluent.Protocol, error) {
	bootstrapSever := "localhost:9092"

	var p *kafka_confluent.Protocol
	var err error
	if receiveTopic != nil {
		p, err = kafka_confluent.New(ctx,
			kafka_confluent.WithConfigMap(&kafka.ConfigMap{
				"bootstrap.servers":  bootstrapSever,
				"group.id":           TEST_GROUP_ID,
				"auto.offset.reset":  "earliest",
				"enable.auto.commit": "true",
			}),
			kafka_confluent.WithReceiverTopics(receiveTopic),
		)
	}
	if sendTopic != "" {
		p, err = kafka_confluent.New(ctx,
			kafka_confluent.WithConfigMap(&kafka.ConfigMap{
				"bootstrap.servers": bootstrapSever,
			}),
			kafka_confluent.WithSenderTopic(sendTopic),
		)
	}
	return p, err
}
