package asyncapi

import (
	asyncapiv3 "github.com/lerenn/asyncapi-codegen/pkg/asyncapi/v3"
)

func ExtractSendOperations(
	spec *asyncapiv3.Specification,
) map[string]*asyncapiv3.Operation {
	operations := make(map[string]*asyncapiv3.Operation)

	for key, op := range spec.Operations {
		if op.Action.IsSend() {
			operations[key] = op
		}
	}

	return operations
}

func ExtractReceiveOperations(
	spec *asyncapiv3.Specification,
) map[string]*asyncapiv3.Operation {
	operations := make(map[string]*asyncapiv3.Operation)

	for key, op := range spec.Operations {
		if op.Action.IsReceive() {
			operations[key] = op
		}
	}

	return operations
}
