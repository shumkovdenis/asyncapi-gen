package boilerplate

import (
	"context"
	"errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/hamba/avro/v2/registry"
)

type EventBus struct {
	eventBus *cqrs.EventBus
}

func NewEventBus(
	brokers []string, registryClient *registry.Client,
) (*EventBus, error) {
	logger := watermill.NewStdLogger(false, false)

	sp := &schemaProvider{
		RegistryClient: registryClient,
	}

	err := sp.RegisterSchemes()
	if err != nil {
		return nil, err
	}

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   brokers,
			Marshaler: kafka.DefaultMarshaler{},
		},
		logger,
	)
	if err != nil {
		return nil, err
	}

	eventBusConfig := cqrs.EventBusConfig{
		GeneratePublishTopic: generatePublishTopic,
		Marshaler: &avroMarshaler{
			schemaProvider: sp,
		},
		Logger: logger,
	}

	eventBus, err := cqrs.NewEventBusWithConfig(publisher, eventBusConfig)
	if err != nil {
		return nil, err
	}

	return &EventBus{eventBus: eventBus}, err
}

func (b EventBus) SendMsgA(ctx context.Context, msg *MsgA) error {
	return b.eventBus.Publish(ctx, msg)
}

func (b EventBus) SendMsgB(ctx context.Context, msg *MsgB) error {
	return b.eventBus.Publish(ctx, msg)
}

func (b EventBus) SendMsgC(ctx context.Context, msg *MsgC) error {
	return b.eventBus.Publish(ctx, msg)
}

func generatePublishTopic(
	params cqrs.GenerateEventPublishTopicParams,
) (string, error) {
	switch params.Event.(type) {
	case *MsgA:
		return topicAB, nil
	case *MsgB:
		return topicAB, nil
	case *MsgC:
		return topicC, nil
	}

	return "", errors.New("unknown message")
}
