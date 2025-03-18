package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	graphql "github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/graph-gophers/graphql-transport-ws/graphqlws"

	"git.imz.world/event-console/live-tracking-microservice/config"
	"git.imz.world/event-console/live-tracking-microservice/kafka"
)

type query struct{}

func (query) Hello() string { return "Hello, world!" }

type subscription struct{}

func (subscription) Track(args struct {
	GroupID string
	Imeis   *[]string
	Topic   *string // Optional topic parameter
}) <-chan string {
	// Determine the topic to use
	var topic string
	if args.Topic != nil {
		topic = *args.Topic
	}

	fmt.Printf("Track subscribed with Group ID: %s, IMEIs: %v, Topic: %s\n",
		args.GroupID,
		args.Imeis,
		topic)

	// Create buffered channel to prevent blocking
	ch := make(chan string, 100)

	go func() {
		defer close(ch)

		// Pass topic as an optional parameter
		messages, errChan, err := kafka.Subscribe(
			args.GroupID,
			args.Imeis,
			topic, // Optional topic
		)
		if err != nil {
			log.Printf("Error subscribing: %s", err)
			return
		}

		// Handle errors from Kafka
		go func() {
			for kafkaErr := range errChan {
				log.Printf("Kafka subscription error: %v", kafkaErr)
			}
		}()

		// Process messages immediately as they arrive
		for msg := range messages {
			fmt.Println("Received Kafka Message:", msg)
			// Non-blocking send to channel - prevents potential deadlocks
			select {
			case ch <- msg:
				// Message sent successfully
			default:
				// Channel is full, log this situation
				log.Printf("Warning: Channel full, dropping message")
			}
		}
	}()

	return ch
}

func main() {
	// Setup logging
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Load configuration
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %s", err)
	}

	// Initialize Kafka with error handling
	if err := kafka.InitKafka(cfg); err != nil {
		log.Fatalf("Failed to initialize Kafka: %s", err)
	}
	// Ensure Kafka connections are closed

	// Initialize GraphQL schema and resolver
	s := `
        schema {
            query: Query
            subscription: Subscription
        }
        type Query {
            hello: String!
        }
        type Subscription {
    track(groupId: String!, imeis: [String!], topic: String): String!
}
    `
	schema := graphql.MustParseSchema(s, &struct {
		query
		subscription
	}{})

	// Set up HTTP server with Gorilla Mux
	r := mux.NewRouter()

	// Set up GraphQL Playground handler
	playgroundHandler := playground.Handler("GraphQL Playground", "/playground")

	// Set up GraphQL WebSocket handler
	graphQLHandler := graphqlws.NewHandlerFunc(schema, &relay.Handler{Schema: schema})

	r.Handle("/query", &relay.Handler{Schema: schema})
	r.Handle("/playground", playgroundHandler)
	r.Handle("/subscriptions", graphQLHandler)

	// Add CORS support with more secure defaults
	corsHandler := handlers.CORS(
		handlers.AllowedOrigins([]string{"http://localhost:5173", "https://yourdomain.com"}),
		handlers.AllowedMethods([]string{"GET", "POST", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Content-Type", "Authorization"}),
		handlers.AllowCredentials(),
	)(r)

	// Graceful shutdown setup
	srv := &http.Server{
		Addr:         ":9090",
		Handler:      corsHandler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Create a channel to listen for interrupt signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Start server in a separate goroutine
	go func() {
		log.Println("Starting server on :9090...")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Block until a signal is received
	<-stop

	// Create a context with a timeout for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Println("Shutting down server gracefully...")
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	log.Println("Server stopped")
}
