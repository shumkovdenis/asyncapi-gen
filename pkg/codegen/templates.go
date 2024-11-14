package codegen

import (
	"embed"
	"maps"
	"path"
	"text/template"

	"github.com/ettle/strcase"
	"github.com/lerenn/asyncapi-codegen/pkg/codegen/generators/v2/templates"
)

const (
	templatesDir = "templates"

	mainTemplatePath                 = templatesDir + "/main.tmpl"
	importsTemplatePath              = templatesDir + "/imports.tmpl"
	eventBusTemplatePath             = templatesDir + "/event_bus.tmpl"
	eventBusCommonTemplatePath       = templatesDir + "/event_bus_common.tmpl"
	eventProcessorTemplatePath       = templatesDir + "/event_processor.tmpl"
	schemaProviderTemplatePath       = templatesDir + "/schema_provider.tmpl"
	schemaProviderCommonTemplatePath = templatesDir + "/schema_provider_common.tmpl"
	avroMarshalerTemplatePath        = templatesDir + "/avro_marshaler.tmpl"
)

//go:embed templates/*
var files embed.FS

func loadTemplate() (*template.Template, error) {
	paths := []string{
		mainTemplatePath,
		importsTemplatePath,
		eventBusTemplatePath,
		eventBusCommonTemplatePath,
		eventProcessorTemplatePath,
		schemaProviderTemplatePath,
		schemaProviderCommonTemplatePath,
		avroMarshalerTemplatePath,
	}

	funcs := helpersFunctions()
	maps.Copy(funcs, templates.HelpersFunctions())

	return template.
		New(path.Base(paths[0])).
		Funcs(funcs).
		ParseFS(files, paths...)
}

func helpersFunctions() template.FuncMap {
	return template.FuncMap{
		"pascal": strcase.ToPascal,
	}
}
