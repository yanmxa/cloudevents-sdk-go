/*
 Copyright 2023 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_confluent

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"github.com/confluentinc/confluent-kafka-go/kafka"

	cecontext "github.com/cloudevents/sdk-go/v2/context"
)

var (
	_ protocol.Sender   = (*Protocol)(nil)
	_ protocol.Opener   = (*Protocol)(nil)
	_ protocol.Receiver = (*Protocol)(nil)
	_ protocol.Closer   = (*Protocol)(nil)
)

type Protocol struct {
	kafkaConfigMap *kafka.ConfigMap

	consumer            *kafka.Consumer
	consumerTopics      []string
	consumerRebalanceCb kafka.RebalanceCb // optional
	consumerPollTimeout int               // optional
	consumerMux         sync.Mutex

	producer                 *kafka.Producer
	producerDefaultTopic     string           // optional
	producerDefaultPartition int32            // optional
	producerDeliveryChan     chan kafka.Event // optional

	// receiver
	incoming chan *kafka.Message
}

func New(ctx context.Context, config *kafka.ConfigMap, opts ...Option) (*Protocol, error) {
	if config == nil {
		return nil, fmt.Errorf("the kafka.ConfigMap must not be nil")
	}

	p := &Protocol{
		kafkaConfigMap:           config,
		producerDefaultPartition: kafka.PartitionAny,
		consumerPollTimeout:      100,
	}
	if err := p.applyOptions(opts...); err != nil {
		return nil, err
	}

	if p.consumerTopics != nil {
		consumer, err := kafka.NewConsumer(p.kafkaConfigMap)
		if err != nil {
			return nil, err
		}
		p.consumer = consumer
	} else {
		producer, err := kafka.NewProducer(p.kafkaConfigMap)
		if err != nil {
			return nil, err
		}
		p.producer = producer
	}
	return p, nil
}

func (p *Protocol) applyOptions(opts ...Option) error {
	for _, fn := range opts {
		if err := fn(p); err != nil {
			return err
		}
	}
	return nil
}

func (p *Protocol) Send(ctx context.Context, in binding.Message, transformers ...binding.Transformer) error {
	if p.producer == nil {
		return fmt.Errorf("the producer client must not be nil")
	}

	var err error
	defer in.Finish(err)

	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &p.producerDefaultTopic,
			Partition: p.producerDefaultPartition,
		},
	}

	if topic := cecontext.TopicFrom(ctx); topic != "" {
		kafkaMsg.TopicPartition.Topic = &topic
	}

	if partition := cecontext.TopicPartitionFrom(ctx); partition != -1 {
		kafkaMsg.TopicPartition.Partition = partition
	}

	if messageKey := cecontext.MessageKeyFrom(ctx); messageKey != "" {
		kafkaMsg.Key = []byte(messageKey)
	}

	err = WriteProducerMessage(ctx, in, kafkaMsg, transformers...)
	if err != nil {
		return err
	}

	return p.producer.Produce(kafkaMsg, p.producerDeliveryChan)
}

func (p *Protocol) OpenInbound(ctx context.Context) error {
	if p.consumer == nil {
		return fmt.Errorf("the consumer client must not be nil")
	}
	p.consumerMux.Lock()
	defer p.consumerMux.Unlock()

	logger := cecontext.LoggerFrom(ctx)

	logger.Infof("Subscribing to topics: %v", p.consumerTopics)
	err := p.consumer.SubscribeTopics(p.consumerTopics, p.consumerRebalanceCb)
	if err != nil {
		return err
	}

	run := true
	for run {
		select {
		case <-ctx.Done():
			run = false
		default:
			ev := p.consumer.Poll(p.consumerPollTimeout)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				p.incoming <- e
			case kafka.Error:
				// Errors should generally be considered informational, the client
				// will try to automatically recover.
				logger.Warnf("%% Error: %v: %v\n", e.Code(), e)
			default:
				logger.Infof("Ignored %v\n", e)
			}
		}
	}

	logger.Infof("Closing consumer %v", p.consumerTopics)
	return p.consumer.Close()
}

// Receive implements Receiver.Receive
func (p *Protocol) Receive(ctx context.Context) (binding.Message, error) {
	select {
	case m, ok := <-p.incoming:
		if !ok {
			return nil, io.EOF
		}
		msg := NewMessage(m)
		return msg, nil
	case <-ctx.Done():
		return nil, io.EOF
	}
}

func (p *Protocol) Close(ctx context.Context) error {
	if p.consumer != nil {
		return p.consumer.Close()
	}
	if p.producer != nil {
		p.producer.Close()
	}
	return nil
}
