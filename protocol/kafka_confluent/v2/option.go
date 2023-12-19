/*
 Copyright 2023 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_confluent

import (
	"context"
	"fmt"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Option is the function signature required to be considered an kafka_confluent.Option.
type Option func(*Protocol) error

func WithConfigMap(config *kafka.ConfigMap) Option {
	return func(p *Protocol) error {
		if config == nil {
			return fmt.Errorf("the kafka.ConfigMap option must not be nil")
		}
		p.kafkaConfigMap = config
		return nil
	}
}

// WithSenderTopic sets the defaultTopic for the kafka.Producer. This option is not required.
func WithSenderTopic(defaultTopic string) Option {
	return func(p *Protocol) error {
		if defaultTopic == "" {
			return fmt.Errorf("the producer topic option must not be nil")
		}
		p.producerDefaultTopic = defaultTopic
		return nil
	}
}

// WithDeliveryChan sets the deliveryChan for the kafka.Producer. This option is not required.
func WithDeliveryChan(deliveryChan chan kafka.Event) Option {
	return func(p *Protocol) error {
		if deliveryChan == nil {
			return fmt.Errorf("the producer deliveryChan option must not be nil")
		}
		p.producerDeliveryChan = deliveryChan
		return nil
	}
}

func WithReceiverTopics(topics []string) Option {
	return func(p *Protocol) error {
		if topics == nil {
			return fmt.Errorf("the consumer topics option must not be nil")
		}
		p.consumerTopics = topics
		return nil
	}
}

func WithRebalanceCallBack(rebalanceCb kafka.RebalanceCb) Option {
	return func(p *Protocol) error {
		if rebalanceCb == nil {
			return fmt.Errorf("the consumer group rebalance callback must not be nil")
		}
		p.consumerRebalanceCb = rebalanceCb
		return nil
	}
}

func WithPollTimeout(timeoutMs int) Option {
	return func(p *Protocol) error {
		p.consumerPollTimeout = timeoutMs
		return nil
	}
}

func WithSender(producer *kafka.Producer) Option {
	return func(p *Protocol) error {
		if producer == nil {
			return fmt.Errorf("the producer option must not be nil")
		}
		p.producer = producer
		return nil
	}
}

func WithReceiver(consumer *kafka.Consumer) Option {
	return func(p *Protocol) error {
		if consumer == nil {
			return fmt.Errorf("the consumer option must not be nil")
		}
		p.consumer = consumer
		return nil
	}
}

// Opaque key type used to store offsets: assgin offset from ctx, commit offset from context
type commitOffsetType struct{}

var offsetKey = commitOffsetType{}

// CommitOffsetCtx will return the topic partitions to commit offsets for.
func CommitOffsetCtx(ctx context.Context, topicPartitions []kafka.TopicPartition) context.Context {
	return context.WithValue(ctx, offsetKey, topicPartitions)
}

// CommitOffsetCtx looks in the given context and returns `[]kafka.TopicPartition` if found and valid, otherwise nil.
func CommitOffsetFrom(ctx context.Context) []kafka.TopicPartition {
	c := ctx.Value(offsetKey)
	if c != nil {
		if s, ok := c.([]kafka.TopicPartition); ok {
			return s
		}
	}
	return nil
}

const (
	OffsetEventSource = "io.cloudevents.kafka.confluent.consumer"
	OffsetEventType   = "io.cloudevents.kafka.confluent.consumer.offsets"
)

func NewOffsetEvent() cloudevents.Event {
	e := cloudevents.NewEvent()
	e.SetSource(OffsetEventSource)
	e.SetType(OffsetEventType)
	return e
}
