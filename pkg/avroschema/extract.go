package avroschema

import (
	"encoding/json"
	"errors"
	"os"
	"path"

	"github.com/ghodss/yaml"
	"github.com/hamba/avro/v2"
)

type specification struct {
	Components components `json:"components"`
}

type components struct {
	Messages map[string]*message `json:"messages"`
}

type message struct {
	Name    string  `json:"name"`
	Payload payload `json:"payload"`
}

type payload struct {
	Schema schema `json:"schema"`
}

type schema struct {
	Ref string `json:"$ref"`
}

func Exctract(filePath string) (map[string]avro.NamedSchema, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	data, err = yaml.YAMLToJSON(data)
	if err != nil {
		return nil, err
	}

	var spec specification

	err = json.Unmarshal(data, &spec)
	if err != nil {
		return nil, err
	}

	schemas := make(map[string]avro.NamedSchema)

	for msgKey, msg := range spec.Components.Messages {
		schema, err := avro.ParseFiles(
			path.Join(path.Dir(filePath), msg.Payload.Schema.Ref),
		)
		if err != nil {
			return nil, err
		}

		namedSchema, ok := schema.(avro.NamedSchema)
		if !ok {
			return nil, errors.New("schema is not named")
		}

		key := "#/components/messages/" + msgKey

		schemas[key] = namedSchema
	}

	return schemas, nil
}
