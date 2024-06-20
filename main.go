package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	graphql "github.com/graph-gophers/graphql-go"
	"github.com/graph-gophers/graphql-go/relay"
	"github.com/graph-gophers/graphql-transport-ws/graphqlws"

	"git.imz.world/event-console/live-tracking-microservice/config"
	"git.imz.world/event-console/live-tracking-microservice/rabbitmq"
)

type query struct{}

func (query) Hello() string { return "Hello, world!" }

type subscription struct{}

func (subscription) Track(args struct{ Topic string }) <-chan string {
	fmt.Println("Track subscribed to topic:", args.Topic)
	ch := make(chan string)

	go func() {
		messages, err := rabbitmq.SubscribeToTopic(args.Topic)
		if err != nil {
			log.Printf("Error subscribing to topic: %s", err)
			close(ch)
			return
		}

		for msg := range messages {
			fmt.Println("msg:", msg)
			var messageData map[string]interface{}
			if err := json.Unmarshal([]byte(msg), &messageData); err != nil {
				log.Printf("Error unmarshalling message: %s", err)
				continue
			}
			ch <- string(msg)
		}
		close(ch)
	}()
	return ch
}

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load configuration: %s", err)
	}

	// Initialize RabbitMQ
	rabbitmq.InitRabbitMQ(cfg)

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
            track(topic: String!): String!
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

	// Serve the GraphQL Playground UI
	r.Handle("/playground", playgroundHandler)

	// Serve the WebSocket handler for subscriptions
	r.Handle("/subscriptions", graphQLHandler)

	// Add CORS support
	corsHandler := handlers.CORS(
		handlers.AllowedOrigins([]string{"*"}),
		handlers.AllowedMethods([]string{"GET", "POST", "OPTIONS"}),
		handlers.AllowedHeaders([]string{"Content-Type", "Authorization"}),
	)(r)

	log.Println("Starting server on :9090...")
	log.Fatal(http.ListenAndServe(":9090", corsHandler))
}
