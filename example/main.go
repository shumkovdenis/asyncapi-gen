package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/hamba/avro/v2/registry"
	"github.com/shumkovdenis/asyncapi-gen/example/gen"
)

type handler struct{}

func (h *handler) ReceiveOrderCancelled(
	ctx context.Context, msg *gen.OrderCancelledEvent,
) error {
	fmt.Printf("received message: %+v\n", msg)

	return nil
}

func (h *handler) ReceiveOrderCreated(
	ctx context.Context, msg *gen.OrderCreatedEvent,
) error {
	fmt.Printf("received message: %+v\n", msg)

	return nil
}

func (h *handler) ReceiveUpdateInventory(
	ctx context.Context, msg *gen.UpdateInventoryCommand,
) error {
	fmt.Printf("received message: %+v\n", msg)

	return nil
}

func main() {
	brokers := []string{"kafka:9092"}
	schemaRegistryURL := "http://schema-registry:8081"

	client, err := registry.NewClient(schemaRegistryURL)
	if err != nil {
		panic(err)
	}

	router, err := gen.NewRouter(brokers, client, &handler{})
	if err != nil {
		panic(err)
	}

	eventBus, err := gen.NewEventBus(brokers, client)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			err := eventBus.SendUpdateInventory(
				context.Background(),
				&gen.UpdateInventoryCommand{
					ProductID:      "1",
					WarehouseID:    "1",
					QuantityChange: 1,
				})
			if err != nil {
				log.Printf("failed to send message: %v", err)
			}

			time.Sleep(500 * time.Millisecond)

			err = eventBus.SendOrderCreated(
				context.Background(),
				&gen.OrderCreatedEvent{
					OrderID: "10",
					UserID:  "20",
					Amount:  3,
				})
			if err != nil {
				log.Printf("failed to send message: %v", err)
			}

			time.Sleep(500 * time.Millisecond)

			err = eventBus.SendOrderCancelled(
				context.Background(),
				&gen.OrderCancelledEvent{
					OrderID: "100",
					UserID:  "200",
				})
			if err != nil {
				log.Printf("failed to send message: %v", err)
			}

			fmt.Println("sent messages")

			time.Sleep(1 * time.Second)
		}
	}()

	err = router.Run(context.Background())
	if err != nil {
		panic(err)
	}
}
