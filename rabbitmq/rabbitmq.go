package rabbitmq

import (
	"fmt"
	"log"
	"strings"

	"git.imz.world/event-console/live-tracking-microservice/config"
	"github.com/rabbitmq/amqp091-go"
)

var rabbitConn *amqp091.Connection
var rabbitCh *amqp091.Channel

// InitRabbitMQ initializes the RabbitMQ connection and channel
func InitRabbitMQ(cfg *config.Config) {
	var err error
	rabbitConn, err = amqp091.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to RabbitMQ: %s", err))
	}
	rabbitCh, err = rabbitConn.Channel()
	if err != nil {
		panic(fmt.Sprintf("Failed to open a channel: %s", err))
	}

	log.Println("RabbitMQ initialized and channel opened")
}

// SubscribeToTopic subscribes to a RabbitMQ topic and returns a channel for GraphQL subscription
func SubscribeToTopic(topic string) (<-chan string, error) {
	fmt.Println("Subscribing to RabbitMQ topic:", topic)
	messages := make(chan string)

	// Validate topic
	if !isValidTopic(topic) {
		return nil, fmt.Errorf("invalid topic: %s", topic)
	}

	// Declare a unique queue for this subscription
	q, err := rabbitCh.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Printf("Failed to declare a queue: %s", err)
		return nil, err
	}

	err = rabbitCh.QueueBind(
		q.Name,          // queue name
		topic,           // routing key
		"live_tracking", // exchange
		false,
		nil,
	)
	if err != nil {
		log.Printf("Failed to bind queue: %s", err)
		return nil, err
	}

	msgs, err := rabbitCh.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Printf("Failed to register a consumer: %s", err)
		return nil, err
	}

	go func() {
		defer close(messages)
		fmt.Println("Waiting for messages from RabbitMQ...")
		for d := range msgs {
			// log.Printf("Received a message: %s", d.Body)
			messages <- string(d.Body)
		}
	}()
	return messages, nil
}

func isValidTopic(topic string) bool {
	// Check if the topic starts with "track."
	if !strings.HasPrefix(topic, "track.") {
		return false
	}

	// Extract the IMEI part of the topic
	parts := strings.Split(topic, ".")
	if len(parts) < 2 {
		return false
	}

	imei := parts[1]

	// Check if the IMEI is a non-empty string
	if imei == "" {
		return false
	}

	return true
}
