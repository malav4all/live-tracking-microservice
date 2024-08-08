package rabbitmq

import (
	"fmt"
	"log"
	"sync"

	"git.imz.world/event-console/live-tracking-microservice/config"
	"github.com/rabbitmq/amqp091-go"
)

var (
	rabbitConn   *amqp091.Connection
	rabbitCh     *amqp091.Channel
	lastMessages = make(map[string]string)
	mu           sync.Mutex
)

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
func SubscribeToTopic(topic, topicType string) (<-chan string, error) {
	fmt.Println("Subscribing to RabbitMQ topic:", topic)
	messages := make(chan string)

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

	exchange := "live_tracking"
	if topicType == "alert" {
		exchange = "alert_exchange"
	}

	err = rabbitCh.QueueBind(
		q.Name,   // queue name
		topic,    // routing key
		exchange, // exchange
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

		// Send the last known message for the topic to the new subscriber
		mu.Lock()
		if lastMessage, ok := lastMessages[topic]; ok {
			messages <- lastMessage
		}
		mu.Unlock()

		for d := range msgs {
			mu.Lock()
			lastMessages[topic] = string(d.Body)
			mu.Unlock()
			messages <- string(d.Body)
		}
	}()
	return messages, nil
}
