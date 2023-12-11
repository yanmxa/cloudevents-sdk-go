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
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const topic = "test-confluent-topic"

func main() {
	ctx := context.Background()

	receiver, err := kafka_confluent.New(ctx, kafka_confluent.WithConfigMap(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1:9092",
		"group.id":          "test-confluent-group-id",
		"auto.offset.reset": "earliest",
	}), kafka_confluent.WithReceiverTopics([]string{topic}))

	defer receiver.Close(ctx)

	c, err := cloudevents.NewClient(receiver)
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	log.Printf("will listen consuming topic %s\n", topic)
	err = c.StartReceiver(ctx, receive)
	if err != nil {
		log.Fatalf("failed to start receiver: %s", err)
	} else {
		log.Printf("receiver stopped\n")
	}
}

func receive(ctx context.Context, event cloudevents.Event) {
	fmt.Printf("%s", event)
}
