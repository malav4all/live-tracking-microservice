package kafka

import (
	"context"
	"fmt"
	"log"

	"git.imz.world/event-console/live-tracking-microservice/config"
	"github.com/segmentio/kafka-go"
)

var kafkaReader *kafka.Reader
var kafkaWriter *kafka.Writer

// InitKafka initializes Kafka producer and consumer
func InitKafka(cfg *config.Config) {
	// Initialize Kafka Reader (Consumer)
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.Group,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	// Initialize Kafka Writer (Producer)
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(cfg.Kafka.Brokers...),
		Topic:    cfg.Kafka.Topic,
		Balancer: &kafka.LeastBytes{},
	}

	log.Println("Kafka initialized: Reader and Writer created")
}

// SubscribeToTopic subscribes to a Kafka topic and returns a channel for GraphQL subscription
func SubscribeToTopic(topic string) (<-chan string, error) {
	fmt.Println("Subscribing to Kafka topic:", topic)
	messages := make(chan string)

	go func() {
		defer close(messages)
		fmt.Println("Waiting for messages from Kafka...")

		for {
			msg, err := kafkaReader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error reading Kafka message: %s", err)
				continue
			}
			messages <- string(msg.Value)
		}
	}()

	return messages, nil
}

// PublishMessage sends a message to the Kafka topic
func PublishMessage(topic, message string) error {
	err := kafkaWriter.WriteMessages(context.Background(),
		kafka.Message{
			Topic: topic,
			Value: []byte(message),
		},
	)
	if err != nil {
		log.Printf("Failed to send Kafka message: %s", err)
		return err
	}

	fmt.Println("Message sent to Kafka topic:", topic)
	return nil
}

// CloseKafka closes the Kafka connections
func CloseKafka() {
	if kafkaReader != nil {
		kafkaReader.Close()
	}
	if kafkaWriter != nil {
		kafkaWriter.Close()
	}
	log.Println("Kafka connections closed")
}
