package boilerplate

import (
	"context"
	"errors"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/registry"
)

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
	MsgA schemaInfo
	MsgB schemaInfo
	MsgC schemaInfo
}

func (p *schemaProvider) GetSchemaInfo(val any) (schemaInfo, error) {
	switch val.(type) {
	case *MsgA:
		return p.schemaCache.MsgA, nil
	case *MsgB:
		return p.schemaCache.MsgB, nil
	case *MsgC:
		return p.schemaCache.MsgC, nil
	}

	return schemaInfo{}, errors.New("unknown schema")
}

func (p *schemaProvider) GenerateName(v any) string {
	switch v.(type) {
	case *MsgA:
		return "msg"
	case *MsgB:
		return "msg"
	case *MsgC:
		return "msg"
	}

	return "unknown"
}

func (p *schemaProvider) RegisterSchemes() error {
	ctx := context.Background()

	var err error

	p.schemaCache.MsgA, err = p.createSchema(ctx, topicAB, msgASchema)
	if err != nil {
		return err
	}

	p.schemaCache.MsgB, err = p.createSchema(ctx, topicAB, msgBSchema)
	if err != nil {
		return err
	}

	p.schemaCache.MsgC, err = p.createSchema(ctx, topicC, msgCSchema)
	if err != nil {
		return err
	}

	return nil
}
