package rabbitmq

import (
	"encoding/json"
	"fmt"
	"log"

	"git.imz.world/event-console/live-tracking-microservice/config"
	"github.com/streadway/amqp"
)

var rabbitConn *amqp.Connection
var rabbitCh *amqp.Channel

// InitRabbitMQ initializes the RabbitMQ connection and channel
func InitRabbitMQ(cfg *config.Config) {
	var err error
	rabbitConn, err = amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to RabbitMQ: %s", err))
	}

	rabbitCh, err = rabbitConn.Channel()
	if err != nil {
		panic(fmt.Sprintf("Failed to open a channel: %s", err))
	}

	_, err = rabbitCh.QueueDeclare(
		"socket_parser_trackloc_10N(JSON)", // name
		true,                               // durable
		false,                              // delete when unused
		false,                              // exclusive
		false,                              // no-wait
		nil,                                // arguments
	)
	if err != nil {
		panic(fmt.Sprintf("Failed to declare queue: %s", err))
	}
	log.Println("RabbitMQ initialized and queue declared")
}

// SubscribeToTopic subscribes to a RabbitMQ topic and returns a channel for GraphQL subscription
func SubscribeToTopic(topic string) <-chan string {
	fmt.Println("Subscribing to RabbitMQ topic:", topic)
	messages := make(chan string)

	go func() {
		defer close(messages)

		msgs, err := rabbitCh.Consume(
			"socket_parser_trackloc_10N(JSON)", // queue
			"",                                 // consumer
			true,                               // auto-ack
			false,                              // exclusive
			false,                              // no-local
			false,                              // no-wait
			nil,                                // args
		)
		if err != nil {
			log.Printf("Failed to register a consumer: %s", err)
			return
		}

		fmt.Println("Waiting for messages from RabbitMQ...")
		for d := range msgs {
			// log.Printf("Received a message: %s", d.Body)

			// Directly publish the message to the constructed topic
			imei := extractJSONField(d.Body, "Imei")
			log.Printf("Extracted imei: %s", imei)

			if imei != "" {
				// Construct routing key based on the JSON data
				routingKey := fmt.Sprintf("track.%s", imei)
				log.Printf("Constructed routing key: %s", routingKey)

				err := publishToTopic(routingKey, d.Body)
				if err != nil {
					log.Printf("Failed to publish to topic: %s", err)
				}

				if routingKey == topic {
					messages <- string(d.Body)
				}
			} else {
				log.Printf("Invalid message structure: %s", string(d.Body))
			}
		}
	}()

	return messages
}

// extractJSONField extracts a field from a JSON message without full unmarshalling
func extractJSONField(data []byte, field string) string {
	var jsonMap map[string]interface{}
	if err := json.Unmarshal(data, &jsonMap); err != nil {
		log.Printf("Failed to unmarshal JSON: %s", err)
		return ""
	}

	if value, ok := jsonMap[field]; ok {
		return fmt.Sprintf("%v", value)
	}
	return ""
}

// publishToTopic publishes a message to the specified RabbitMQ topic
func publishToTopic(topic string, message []byte) error {
	err := rabbitCh.Publish(
		"live_tracking_exchange", // exchange
		topic,                    // routing key
		false,                    // mandatory
		false,                    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish to topic: %w", err)
	}
	return nil
}
