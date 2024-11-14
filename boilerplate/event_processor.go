package boilerplate

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/hamba/avro/v2/registry"
)

type Handler interface {
	HandleMsgA(ctx context.Context, msg *MsgA) error
	HandleMsgB(ctx context.Context, msg *MsgB) error
	HandleMsgC(ctx context.Context, msg *MsgC) error
}

func NewRouter(
	brokers []string, registryClient *registry.Client, handler Handler,
) (*message.Router, error) {
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}

	router.AddPlugin(plugin.SignalsHandler)

	eventProcessorConfig := cqrs.EventGroupProcessorConfig{
		GenerateSubscribeTopic: func(
			params cqrs.EventGroupProcessorGenerateSubscribeTopicParams,
		) (string, error) {
			return params.EventGroupName, nil
		},
		SubscriberConstructor: func(
			_ cqrs.EventGroupProcessorSubscriberConstructorParams,
		) (message.Subscriber, error) {
			config := kafka.SubscriberConfig{
				Brokers:     brokers,
				Unmarshaler: kafka.DefaultMarshaler{},
			}

			return kafka.NewSubscriber(config, logger)
		},
		Marshaler: &avroMarshaler{
			schemaProvider: &schemaProvider{
				RegistryClient: registryClient,
			},
		},
		Logger: logger,
	}

	eventProcessor, err := cqrs.NewEventGroupProcessorWithConfig(
		router, eventProcessorConfig,
	)
	if err != nil {
		return nil, err
	}

	err = eventProcessor.AddHandlersGroup(topicAB,
		cqrs.NewGroupEventHandler(
			func(ctx context.Context, msg *MsgA) error {
				return handler.HandleMsgA(ctx, msg)
			},
		),
		cqrs.NewGroupEventHandler(
			func(ctx context.Context, msg *MsgB) error {
				return handler.HandleMsgB(ctx, msg)
			},
		),
	)
	if err != nil {
		return nil, err
	}

	err = eventProcessor.AddHandlersGroup(topicC, cqrs.NewGroupEventHandler(
		func(ctx context.Context, msg *MsgC) error {
			return handler.HandleMsgC(ctx, msg)
		},
	))
	if err != nil {
		return nil, err
	}

	return router, nil
}
