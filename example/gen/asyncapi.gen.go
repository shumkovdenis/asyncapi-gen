// Code generated by ogen, DO NOT EDIT.
package gen

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/plugin"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/hamba/avro/v2"
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

func (b EventBus) SendOrderCancelled(
	ctx context.Context, 
	msg *OrderCancelledEvent,
) error {
	return b.eventBus.Publish(ctx, msg)
}

func (b EventBus) SendOrderCreated(
	ctx context.Context, 
	msg *OrderCreatedEvent,
) error {
	return b.eventBus.Publish(ctx, msg)
}

func (b EventBus) SendUpdateInventory(
	ctx context.Context, 
	msg *UpdateInventoryCommand,
) error {
	return b.eventBus.Publish(ctx, msg)
}

func generatePublishTopic(
	params cqrs.GenerateEventPublishTopicParams,
) (string, error) {
	switch params.Event.(type) {
	case *OrderCancelledEvent:
		return "order", nil
	case *OrderCreatedEvent:
		return "order", nil
	case *UpdateInventoryCommand:
		return "inventory.update", nil
	}

	return "", errors.New("unknown message")
}
type Handler interface {
	ReceiveOrderCancelled(ctx context.Context, msg *OrderCancelledEvent) error
	ReceiveOrderCreated(ctx context.Context, msg *OrderCreatedEvent) error
	ReceiveUpdateInventory(ctx context.Context, msg *UpdateInventoryCommand) error
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
	err = eventProcessor.AddHandlersGroup("order",
		cqrs.NewGroupEventHandler(
			func(ctx context.Context, msg *OrderCancelledEvent) error {
				return handler.ReceiveOrderCancelled(ctx, msg)
			},
		),
		cqrs.NewGroupEventHandler(
			func(ctx context.Context, msg *OrderCreatedEvent) error {
				return handler.ReceiveOrderCreated(ctx, msg)
			},
		),
	)
	if err != nil {
		return nil, err
	}
	err = eventProcessor.AddHandlersGroup("inventory.update",
		cqrs.NewGroupEventHandler(
			func(ctx context.Context, msg *UpdateInventoryCommand) error {
				return handler.ReceiveUpdateInventory(ctx, msg)
			},
		),
	)
	if err != nil {
		return nil, err
	}

	return router, nil
}

type avroMarshaler struct {
	schemaProvider *schemaProvider
}

func (m *avroMarshaler) Marshal(val any) (*message.Message, error) {
	schemaInfo, err := m.schemaProvider.GetSchemaInfo(val)
	if err != nil {
		return nil, err
	}

	payload, err := avro.Marshal(schemaInfo.Schema, val)
	if err != nil {
		return nil, err
	}

	b, err := wireMessage(schemaInfo.ID, payload)
	if err != nil {
		return nil, err
	}

	msg := message.NewMessage(
		watermill.NewUUID(),
		b,
	)

	return msg, nil
}

func (m *avroMarshaler) Unmarshal(msg *message.Message, val any) (err error) {
	schemaID, err := extractSchemaID(msg.Payload)
	if err != nil {
		return err
	}

	schema, err := m.schemaProvider.GetSchema(schemaID)
	if err != nil {
		return err
	}

	payload, err := extractPayload(msg.Payload)
	if err != nil {
		return err
	}

	return avro.Unmarshal(schema, payload, val)
}

func (m *avroMarshaler) Name(cmdOrEvent any) string {
	return m.schemaProvider.GenerateName(cmdOrEvent)
}

func (m *avroMarshaler) NameFromMessage(msg *message.Message) string {
	schemaID, err := extractSchemaID(msg.Payload)
	if err != nil {
		return "invalid"
	}

	name, err := m.schemaProvider.GetName(schemaID)
	if err != nil {
		return "unknown"
	}

	return name
}

const (
	magicByte byte = 0x0

	wireSize = 5
	idSize   = 4
)

func wireMessage(id uint32, payload []byte) ([]byte, error) {
	var buf bytes.Buffer

	err := buf.WriteByte(magicByte)
	if err != nil {
		return nil, err
	}

	idBytes := make([]byte, idSize)
	binary.BigEndian.PutUint32(idBytes, id)

	_, err = buf.Write(idBytes)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(payload)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func extractSchemaID(data []byte) (uint32, error) {
	err := checkWireFormat(data)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint32(data[1:5]), nil
}

func extractPayload(data []byte) ([]byte, error) {
	err := checkWireFormat(data)
	if err != nil {
		return nil, err
	}

	return data[wireSize:], nil
}

func checkWireFormat(data []byte) error {
	if len(data) < wireSize {
		return errors.New("data too short")
	}

	if data[0] != magicByte {
		return fmt.Errorf("invalid magic byte: %x", data[0])
	}

	return nil
}

type schemaProvider struct {
	RegistryClient *registry.Client

	schemaCache schemaInfoCache
	nameCache   sync.Map
}

type schemaInfo struct {
	ID     uint32
	Schema avro.Schema
}

func (p *schemaProvider) GetSchema(id uint32) (avro.Schema, error) {
	return p.RegistryClient.GetSchema(context.Background(), int(id))
}

func (p *schemaProvider) GetName(id uint32) (string, error) {
	nameVal, ok := p.nameCache.Load(id)
	if ok {
		name, ok := nameVal.(string)
		if ok {
			return name, nil
		}
	}

	schema, err := p.RegistryClient.GetSchema(context.Background(), int(id))
	if err != nil {
		return "", err
	}

	name, err := schemaName(schema)
	if err != nil {
		return "", err
	}

	p.nameCache.Store(id, name)

	return name, nil
}

func (p *schemaProvider) createSchema(
	ctx context.Context, topic string, dataSchema string,
) (schemaInfo, error) {
	schema, err := avro.Parse(dataSchema)
	if err != nil {
		return schemaInfo{}, err
	}

	name, err := schemaName(schema)
	if err != nil {
		return schemaInfo{}, err
	}

	subject := subjectName(topic, name)

	id, schema, err := p.RegistryClient.CreateSchema(ctx, subject, dataSchema)
	if err != nil {
		return schemaInfo{}, err
	}

	return schemaInfo{
		ID:     uint32(id),
		Schema: schema,
	}, nil
}

func schemaName(schema avro.Schema) (string, error) {
	namedSchema, ok := schema.(avro.NamedSchema)
	if !ok {
		return "", errors.New("schema is not named")
	}

	return namedSchema.FullName(), nil
}

func subjectName(topic string, name string) string {
	return topic + "-" + name
}

type schemaInfoCache struct {
	OrderCancelledEvent schemaInfo
	OrderCreatedEvent schemaInfo
	UpdateInventoryCommand schemaInfo
}

func (p *schemaProvider) GetSchemaInfo(val any) (schemaInfo, error) {
	switch val.(type) {
	case *OrderCancelledEvent:
		return p.schemaCache.OrderCancelledEvent, nil
	case *OrderCreatedEvent:
		return p.schemaCache.OrderCreatedEvent, nil
	case *UpdateInventoryCommand:
		return p.schemaCache.UpdateInventoryCommand, nil
	}

	return schemaInfo{}, errors.New("unknown schema")
}

func (p *schemaProvider) GenerateName(v any) string {
	switch v.(type) {
	case *OrderCancelledEvent:
		return "OrderCancelledEvent"
	case *OrderCreatedEvent:
		return "OrderCreatedEvent"
	case *UpdateInventoryCommand:
		return "UpdateInventoryCommand"
	}

	return "unknown"
}

func (p *schemaProvider) RegisterSchemes() error {
	ctx := context.Background()

	var err error

	p.schemaCache.OrderCancelledEvent, err = p.createSchema(ctx, "order", `{"name":"OrderCancelledEvent","type":"record","fields":[{"name":"orderId","type":"string"},{"name":"userId","type":"string"}]}`)
	if err != nil {
		return err
	}

	p.schemaCache.OrderCreatedEvent, err = p.createSchema(ctx, "order", `{"name":"OrderCreatedEvent","type":"record","fields":[{"name":"orderId","type":"string"},{"name":"userId","type":"string"},{"name":"amount","type":"int"}]}`)
	if err != nil {
		return err
	}

	p.schemaCache.UpdateInventoryCommand, err = p.createSchema(ctx, "inventory.update", `{"name":"UpdateInventoryCommand","type":"record","fields":[{"name":"productId","type":"string"},{"name":"warehouseId","type":"string"},{"name":"quantityChange","type":"int"}]}`)
	if err != nil {
		return err
	}

	return nil
}
