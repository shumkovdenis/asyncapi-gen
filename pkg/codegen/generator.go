package codegen

import (
	"bytes"

	"github.com/hamba/avro/v2"
	asyncapi "github.com/lerenn/asyncapi-codegen/pkg/asyncapi/v3"
)

type Generator struct {
	EventBusOperations       map[string]*asyncapi.Operation
	EventProcessorOperations map[string]*asyncapi.Operation
	Channels                 map[string]*asyncapi.Channel
	AvroSchemas              map[string]avro.NamedSchema
	PackageName              string
}

func (g Generator) Generate() (string, error) {
	tmpl, err := loadTemplate()
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	if err := tmpl.Execute(buf, g); err != nil {
		return "", err
	}

	return buf.String(), nil
}
