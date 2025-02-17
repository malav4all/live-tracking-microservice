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

// InitKafka initializes the Kafka consumer and producer
func InitKafka(cfg *config.Config) {
	// Initialize Kafka Reader (Consumer)
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		GroupID:  cfg.Kafka.Group,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	// Initialize Kafka Writer (Producer)
	kafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP(cfg.Kafka.Brokers...),
		Topic:    "live_tracking", // Default topic for tracking data
		Balancer: &kafka.LeastBytes{},
	}

	log.Println("Kafka initialized: Reader and Writer created")
}

// Subscribe listens to Kafka messages based on groupId and imeis
func Subscribe(groupID string, imeis *[]string) (<-chan string, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  kafkaReader.Config().Brokers,
		GroupID:  groupID, // Dynamic group ID
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	messages := make(chan string)

	go func() {
		defer close(messages)
		fmt.Println("Waiting for messages from Kafka...")

		for {
			msg, err := reader.ReadMessage(context.Background())
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

// CloseKafka closes the Kafka consumer and producer
func CloseKafka() {
	if kafkaReader != nil {
		kafkaReader.Close()
	}
	if kafkaWriter != nil {
		kafkaWriter.Close()
	}
	log.Println("Kafka connections closed")
}
