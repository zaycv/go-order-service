package kafka

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
	"github.com/zaycv/go-order-service/internal/cache"
	"github.com/zaycv/go-order-service/internal/storage"
)

// ConsumeKafka слушает Kafka и сохраняет заказы в БД и кеш
func ConsumeKafka(store *storage.Storage, c *cache.Cache) {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "orders"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   topic,
		GroupID: "order-service",
	})

	log.Println("Kafka consumer started...")

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("error reading kafka message: %v", err)
			continue
		}
		log.Printf("got message: %s", string(m.Value))

		var o storage.Order
		if err := json.Unmarshal(m.Value, &o); err != nil {
			log.Printf("failed to unmarshal order: %v", err)
			continue
		}

		if err := store.SaveOrder(o); err != nil {
			log.Printf("failed to save order: %v", err)
			continue
		}

		// добавляем в кеш
		if c != nil {
			c.Set(o)
		}
	}
}
