package asyncapi

import (
	"github.com/lerenn/asyncapi-codegen/pkg/asyncapi/parser"
	asyncapiv3 "github.com/lerenn/asyncapi-codegen/pkg/asyncapi/v3"
)

func Parse(path string) (*asyncapiv3.Specification, error) {
	tempSpec, err := parser.FromFile(parser.FromFileParams{
		Path: path,
	})
	if err != nil {
		return nil, err
	}

	spec, err := asyncapiv3.FromUnknownVersion(tempSpec)
	if err != nil {
		return nil, err
	}

	err = spec.Process()
	if err != nil {
		return nil, err
	}

	return spec, nil
}
