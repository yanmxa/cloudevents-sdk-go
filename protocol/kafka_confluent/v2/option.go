/*
 Copyright 2023 The CloudEvents Authors
 SPDX-License-Identifier: Apache-2.0
*/

package kafka_confluent

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Option is the function signature required to be considered an kafka_confluent.Option.
type Option func(*Protocol) error

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
