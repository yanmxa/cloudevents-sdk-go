/*
 Copyright 2021 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"fmt"
	"log"
	"strconv"

	kafka_confluent "github.com/cloudevents/sdk-go/protocol/kafka_confluent/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var (
	topic          = "test-confluent-topic"
	offsetToCommit = kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: kafka.OffsetBeginning}
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	receiver, err := kafka_confluent.New(ctx, kafka_confluent.WithConfigMap(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "test-confluent-offset-id",
		// "auto.offset.reset":  "earliest",
		"auto.offset.reset":  "latest",
		"enable.auto.commit": "false",
	}), kafka_confluent.WithReceiverTopics([]string{topic}))

	defer receiver.Close(ctx)

	c, err := cloudevents.NewClient(receiver, client.WithPollGoroutines(1))
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	go func() {
		lastCommitOffset := int64(0)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if lastCommitOffset < int64(offsetToCommit.Offset) {
					fmt.Printf(">> commit offset %s[%d:%d] \n", *offsetToCommit.Topic, offsetToCommit.Partition, offsetToCommit.Offset)
					c.Send(kafka_confluent.CommitOffsetCtx(ctx, []kafka.TopicPartition{offsetToCommit}), cloudevents.NewEvent())
					lastCommitOffset = int64(offsetToCommit.Offset)
				}
			}
		}
	}()

	log.Printf("will listen consuming topic %s\n", topic)
	err = c.StartReceiver(ctx, receive)
	if err != nil {
		log.Fatalf("failed to start receiver: %s", err)
	} else {
		log.Printf("receiver stopped\n")
	}
	cancel()
}

func receive(ctx context.Context, event cloudevents.Event) {
	ext := event.Extensions()
	offset, err := strconv.ParseInt(ext[kafka_confluent.KafkaOffsetKey].(string), 10, 64)
	if err != nil {
		log.Printf("failed to parse offset: %s", err)
		return
	}
	if offset%5 == 0 {
		offsetToCommit.Offset = kafka.Offset(offset)
	}
	fmt.Printf("%s[%s:%s] \n", ext[kafka_confluent.KafkaTopicKey],
		ext[kafka_confluent.KafkaPartitionKey], ext[kafka_confluent.KafkaOffsetKey])
}
