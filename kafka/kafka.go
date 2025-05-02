package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

var (
	kafkaReader *kafka.Reader
	kafkaWriter *kafka.Writer
	mu          sync.Mutex
	// Cache to store last packet for each IMEI
	imeiCache   map[string]string
	imeiCacheMu sync.RWMutex
)

// LoadEnv loads environment variables from .env file
func LoadEnv() error {
	return godotenv.Load()
}

// getEnvWithDefault gets an environment variable with a default value if not set
func getEnvWithDefault(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// InitKafka initializes the Kafka consumer and producer using environment variables
func InitKafka() error {
	// Load environment variables
	if err := LoadEnv(); err != nil {
		log.Printf("Warning: Failed to load .env file: %s", err)
		// Continue execution, will use default values if env vars not set
	}

	mu.Lock()
	defer mu.Unlock()

	// Initialize the caches
	imeiCache = make(map[string]string)

	// Get Kafka brokers from environment
	brokersStr := getEnvWithDefault("KAFKA_BROKERS", "103.20.212.44:9092")
	brokers := strings.Split(brokersStr, ",")

	// Validate configuration
	if len(brokers) == 0 {
		return fmt.Errorf("no Kafka brokers specified")
	}

	// Use a default topic if not specified in environment
	topic := getEnvWithDefault("KAFKA_TOPIC", "default_topic")
	if topic == "" {
		topic = "default_topic"
	}

	// Use a default group ID if not specified in environment
	groupID := getEnvWithDefault("KAFKA_GROUP", "default_group")
	if groupID == "" {
		groupID = "default_group"
	}

	// Initialize Kafka Reader with better error handling
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    1e3,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
		StartOffset: kafka.LastOffset,
		ErrorLogger: kafka.LoggerFunc(log.Printf),
		Dialer: &kafka.Dialer{
			Timeout:   30 * time.Second,
			DualStack: true,
		},
	})

	// Initialize Kafka Writer with better error handling
	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		MaxAttempts:  5,
		BatchTimeout: 5 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		ErrorLogger:  kafka.LoggerFunc(log.Printf),
	}

	// Test connection before returning
	testCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Ping test to check if brokers are reachable
	conn, err := kafka.DialLeader(testCtx, "tcp", brokers[0], topic, 0)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka broker: %w", err)
	}
	conn.Close()

	log.Printf("Kafka initialized: Reader and Writer created (Topic: %s, Group ID: %s)", topic, groupID)
	return nil
}

// GetLastPacketForIMEI returns the last known packet for a given IMEI
func GetLastPacketForIMEI(imei string) (string, bool) {
	imeiCacheMu.RLock()
	defer imeiCacheMu.RUnlock()

	packet, exists := imeiCache[imei]
	return packet, exists
}

// UpdateIMEICache stores the latest packet for an IMEI
func UpdateIMEICache(imei string, packet string) {
	imeiCacheMu.Lock()
	defer imeiCacheMu.Unlock()

	imeiCache[imei] = packet
}

// Subscribe handles subscriptions to Kafka topics with proper handling for different topic types
func Subscribe(groupID string, imeis *[]string, topic ...string) (<-chan string, <-chan error, error) {
	// Use configured topic if no topic is provided
	useTopic := kafkaReader.Config().Topic
	if len(topic) > 0 && topic[0] != "" {
		useTopic = topic[0]
	}

	// Validate group ID
	if groupID == "" {
		groupID = kafkaReader.Config().GroupID
	}

	// Create a unique consumer group ID for each subscription to avoid message loss
	uniqueGroupID := fmt.Sprintf("%s-%d", groupID, time.Now().UnixNano())

	log.Printf("Creating subscription with unique group ID: %s for topic: %s", uniqueGroupID, useTopic)

	// Create reader with the specified or default group ID and topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kafkaReader.Config().Brokers,
		GroupID:     uniqueGroupID,
		Topic:       useTopic,
		MinBytes:    1e3,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
		StartOffset: kafka.LastOffset,
	})

	// Create buffered channels to prevent blocking
	messages := make(chan string, 100)
	errChan := make(chan error, 10)

	// Send immediate latest cached packets for all requested IMEIs
	// But only if we're not on the live_alert topic
	if imeis != nil && len(*imeis) > 0 && useTopic != "live_alert" {
		go func() {
			imeiCacheMu.RLock()
			for _, imei := range *imeis {
				if packet, exists := imeiCache[imei]; exists {
					log.Printf("Sending cached packet for IMEI %s", imei)
					messages <- packet
				}
			}
			imeiCacheMu.RUnlock()
		}()
	}

	go func() {
		defer func() {
			close(messages)
			close(errChan)
			if err := reader.Close(); err != nil {
				log.Printf("Error closing Kafka reader: %v", err)
			}
		}()

		log.Printf("Waiting for messages from Kafka (Topic: %s, Group ID: %s)", useTopic, uniqueGroupID)

		for {
			// Use a shorter context timeout for quicker message processing
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			msg, err := reader.ReadMessage(ctx)
			cancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					// This is expected behavior when no messages are available
					log.Println("No messages received within timeout, continuing...")
					continue
				} else if ctx.Err() != nil {
					// Handle context cancellation gracefully
					log.Println("Context cancelled, continuing...")
					continue
				}

				// Only send actual errors to the error channel
				log.Printf("Error reading Kafka message: %v", err)
				select {
				case errChan <- fmt.Errorf("error reading Kafka message: %w", err):
					// Error sent successfully
				default:
					// Error channel full, just log
					log.Printf("Error channel full, couldn't send: %v", err)
				}

				// Add a short sleep to prevent tight error loops
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Parse the message to check IMEI and apply filtering
			var messageData map[string]interface{}
			if err := json.Unmarshal(msg.Value, &messageData); err != nil {
				log.Printf("Error parsing message: %v", err)
				continue
			}

			// Extract IMEI from message
			imeiValue, ok := messageData["imei"].(string)
			if !ok {
				log.Println("No IMEI found in message")
				continue
			}

			// Handle topic-specific caching
			if useTopic != "live_alert" {
				// For live_tracking, update the cache
				UpdateIMEICache(imeiValue, string(msg.Value))
				log.Printf("Updated tracking cache for IMEI %s", imeiValue)
			} else {
				// For live_alert, we don't cache but log it
				log.Printf("Processing alert for IMEI %s", imeiValue)
			}

			// IMEI filtering
			if imeis != nil && len(*imeis) > 0 {
				matched := false
				for _, filterImei := range *imeis {
					if imeiValue == filterImei {
						matched = true
						break
					}
				}

				if !matched {
					log.Printf("IMEI %s doesn't match filter, skipping", imeiValue)
					continue
				}
			}

			// Send the original message as string immediately
			select {
			case messages <- string(msg.Value):
				// Message sent successfully
				log.Printf("Message for IMEI %s on topic %s forwarded to subscriber", imeiValue, useTopic)
			default:
				// Channel is full, log this situation
				log.Printf("Warning: Messages channel full, dropping message for IMEI %s", imeiValue)
			}
		}
	}()

	return messages, errChan, nil
}

// PublishMessage updated to use default topic if not specified
func PublishMessage(message string, topic ...string) error {
	// Use configured topic if no topic is provided
	useTopic := kafkaWriter.Topic
	if len(topic) > 0 && topic[0] != "" {
		useTopic = topic[0]
	}

	// Validate inputs
	if useTopic == "" {
		return fmt.Errorf("topic cannot be empty")
	}
	if message == "" {
		return fmt.Errorf("message cannot be empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg := kafka.Message{
		Topic: useTopic,
		Value: []byte(message),
	}

	err := kafkaWriter.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to send Kafka message: %w", err)
	}

	log.Println("Message sent to Kafka topic:", useTopic)
	return nil
}
