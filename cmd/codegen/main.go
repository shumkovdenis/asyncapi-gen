package main

import (
	"flag"
	"log"
	"os"
	"path"

	"github.com/shumkovdenis/asyncapi-gen/pkg/asyncapi"
	"github.com/shumkovdenis/asyncapi-gen/pkg/avroschema"
	"github.com/shumkovdenis/asyncapi-gen/pkg/codegen"
)

func main() {
	os.Exit(run(os.Args))
}

func run(args []string) int {
	var (
		packageName string
		outputDir   string
	)

	flgs := flag.NewFlagSet("codegen", flag.ExitOnError)
	flgs.SetOutput(os.Stderr)
	flgs.StringVar(&packageName, "pkg", "", "The package name of the output file.")
	flgs.StringVar(&outputDir, "o", "", "The output directory of the generated files.")

	err := flgs.Parse(args[1:])
	if err != nil {
		log.Println(err)
		return 1
	}

	err = os.MkdirAll(outputDir, 0o755)
	if err != nil {
		log.Println(err)
		return 1
	}

	inputFile := flgs.Arg(0)

	schemas, err := avroschema.Exctract(inputFile)
	if err != nil {
		log.Println(err)
		return 1
	}

	err = avroschema.Generate(schemas, packageName, path.Join(outputDir, "avro.gen.go"))
	if err != nil {
		log.Println(err)
		return 1
	}

	spec, err := asyncapi.Parse(inputFile)
	if err != nil {
		log.Println(err)
		return 1
	}

	gen := codegen.Generator{
		EventBusOperations:       asyncapi.ExtractSendOperations(spec),
		EventProcessorOperations: asyncapi.ExtractReceiveOperations(spec),
		Channels:                 spec.Channels,
		AvroSchemas:              schemas,
		PackageName:              packageName,
	}

	content, err := gen.Generate()
	if err != nil {
		log.Println(err)
		return 1
	}

	err = os.WriteFile(path.Join(outputDir, "asyncapi.gen.go"), []byte(content), 0o644)
	if err != nil {
		log.Println(err)
		return 1
	}

	return 0
}
