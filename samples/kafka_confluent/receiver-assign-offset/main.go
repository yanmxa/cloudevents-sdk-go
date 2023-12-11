/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"fmt"
	"log"

	kafka_confluent "github.com/cloudevents/sdk-go/protocol/kafka_confluent/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var topic = "test-confluent-topic"

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	receiver, err := kafka_confluent.New(ctx, kafka_confluent.WithConfigMap(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "test-confluent-offset-id",
		// "auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	}), kafka_confluent.WithReceiverTopics([]string{topic}))

	defer receiver.Close(ctx)

	c, err := cloudevents.NewClient(receiver, client.WithPollGoroutines(1))
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	offsetToStart := []kafka.TopicPartition{
		{Topic: &topic, Partition: 0, Offset: 495},
	}

	log.Printf("will listen consuming topic %s\n", topic)
	err = c.StartReceiver(kafka_confluent.CommitOffsetCtx(ctx, offsetToStart), receive)
	if err != nil {
		log.Fatalf("failed to start receiver: %s", err)
	} else {
		log.Printf("receiver stopped\n")
	}
	cancel()
}

func receive(ctx context.Context, event cloudevents.Event) {
	ext := event.Extensions()

	fmt.Printf("%s[%s:%s] \n", ext[kafka_confluent.KafkaTopicKey],
		ext[kafka_confluent.KafkaPartitionKey], ext[kafka_confluent.KafkaOffsetKey])
}
