package boilerplate

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/hamba/avro/v2"
)

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
