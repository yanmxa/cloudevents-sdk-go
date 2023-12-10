/*
Copyright 2023 The CloudEvents Authors
SPDX-License-Identifier: Apache-2.0
*/

package kafka_confluent

import (
	"bytes"
	"context"
	"io"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
	"github.com/cloudevents/sdk-go/v2/binding/spec"
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	// used to indicate that the message key is needed
	kafkaMessageKey struct{}
	// extends the kafka.Message to support the interfaces for the converting it to binding.Message
	kafkaMessageWriter kafka.Message
)

var (
	_ binding.StructuredWriter = (*kafkaMessageWriter)(nil)
	_ binding.BinaryWriter     = (*kafkaMessageWriter)(nil)
)

// WritePubMessage fills the provided pubMessage with the message m.
// Using context you can tweak the encoding processing (more details on binding.Write documentation).
func WritePubMessage(ctx context.Context, m binding.Message, kafkaMsg *kafka.Message,
	transformers ...binding.Transformer,
) error {
	structuredWriter := (*kafkaMessageWriter)(kafkaMsg)
	binaryWriter := (*kafkaMessageWriter)(kafkaMsg)

	hasMessageKey := binding.GetOrDefaultFromCtx(ctx, kafkaMessageKey{}, true).(bool)

	// if the message key is needed, then add the transformer to extract it
	var messageKey string
	if hasMessageKey {
		transformers = append(transformers, binding.TransformerFunc(
			func(r binding.MessageMetadataReader, w binding.MessageMetadataWriter) error {
				ext := r.GetExtension(KafkaMessageKey)
				if types.IsZero(ext) {
					return nil
				}
				extStr, err := types.Format(ext)
				if err != nil {
					return err
				}
				messageKey = extStr
				return nil
			}),
		)
	}

	_, err := binding.Write(
		ctx,
		m,
		structuredWriter,
		binaryWriter,
		transformers...,
	)
	if messageKey != "" {
		kafkaMsg.Key = []byte(messageKey)
	}
	return err
}

func WithoutMessageKeyContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, kafkaMessageKey{}, false)
}

func (b *kafkaMessageWriter) SetStructuredEvent(ctx context.Context, f format.Format, event io.Reader) error {
	b.Headers = []kafka.Header{{
		Key:   contentTypeKey,
		Value: []byte(f.MediaType()),
	}}

	var buf bytes.Buffer
	_, err := io.Copy(&buf, event)
	if err != nil {
		return err
	}

	b.Value = buf.Bytes()
	return nil
}

func (b *kafkaMessageWriter) Start(ctx context.Context) error {
	b.Headers = []kafka.Header{}
	return nil
}

func (b *kafkaMessageWriter) End(ctx context.Context) error {
	return nil
}

func (b *kafkaMessageWriter) SetData(reader io.Reader) error {
	buf, ok := reader.(*bytes.Buffer)
	if !ok {
		buf = new(bytes.Buffer)
		_, err := io.Copy(buf, reader)
		if err != nil {
			return err
		}
	}
	b.Value = buf.Bytes()
	return nil
}

func (b *kafkaMessageWriter) SetAttribute(attribute spec.Attribute, value interface{}) error {
	if attribute.Kind() == spec.DataContentType {
		if value == nil {
			b.removeProperty(contentTypeKey)
			return nil
		}
		b.addProperty(contentTypeKey, value)
	} else {
		key := prefix + attribute.Name()
		if value == nil {
			b.removeProperty(key)
			return nil
		}
		b.addProperty(key, value)
	}
	return nil
}

func (b *kafkaMessageWriter) SetExtension(name string, value interface{}) error {
	if value == nil {
		b.removeProperty(prefix + name)
	}
	return b.addProperty(prefix+name, value)
}

func (b *kafkaMessageWriter) removeProperty(key string) {
	for i, v := range b.Headers {
		if v.Key == key {
			b.Headers = append(b.Headers[:i], b.Headers[i+1:]...)
			break
		}
	}
}

func (b *kafkaMessageWriter) addProperty(key string, value interface{}) error {
	s, err := types.Format(value)
	if err != nil {
		return err
	}
	b.Headers = append(b.Headers, kafka.Header{Key: key, Value: []byte(s)})
	return nil
}
