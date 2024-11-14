package avroschema

import (
	"bytes"
	"os"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/gen"
	"golang.org/x/tools/imports"
)

func Generate(
	schemas map[string]avro.NamedSchema,
	packageName string,
	outputPath string,
) error {
	tags := map[string]gen.TagStyle{}

	g := gen.NewGenerator(packageName, tags)

	for _, schema := range schemas {
		g.Parse(schema)
	}

	var buf bytes.Buffer

	err := g.Write(&buf)
	if err != nil {
		return err
	}

	formatted, err := imports.Process("", buf.Bytes(), nil)
	if err != nil {
		return err
	}

	err = os.WriteFile(outputPath, []byte(formatted), 0o644)
	if err != nil {
		panic(err)
	}

	return nil
}
