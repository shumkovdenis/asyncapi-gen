.PHONY: generate
generate:
	go run ./cmd/codegen/main.go -pkg gen -o example/gen example/order-service.yml
