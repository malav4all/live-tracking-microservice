package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

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

func (subscription) Track(args struct {
	AccountId string
	Imeis     *[]string
}) <-chan string {
	fmt.Println("Track subscribed to accountId:", args.AccountId)
	ch := make(chan string)

	go func() {
		defer close(ch)
		var topics []string

		if args.Imeis == nil || len(*args.Imeis) == 0 {
			// If no specific IMEIs are provided, subscribe to all messages for the account
			topics = append(topics, fmt.Sprintf("track.%s", args.AccountId))
		} else {
			// Subscribe to each provided IMEI
			for _, imei := range *args.Imeis {
				topics = append(topics, fmt.Sprintf("track.%s.%s", args.AccountId, imei))
			}
		}

		var wg sync.WaitGroup
		for _, topic := range topics {
			wg.Add(1)
			go func(topic string) {
				defer wg.Done()
				messages, err := rabbitmq.SubscribeToTopic(topic)
				if err != nil {
					log.Printf("Error subscribing to topic: %s", err)
					return
				}
				for msg := range messages {
					fmt.Println("---------------------Update Message Received------------------------", msg)
					var messageData map[string]interface{}
					if err := json.Unmarshal([]byte(msg), &messageData); err != nil {
						log.Printf("Error unmarshalling message: %s", err)
						continue
					}
					ch <- string(msg)
					fmt.Println("---------------------End Message Received------------------------")
				}
			}(topic)
		}
		wg.Wait()
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
            track(accountId: String!, imeis: [String!]): String!
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

	log.Println("Starting server on :7080...")
	log.Fatal(http.ListenAndServe(":7080", corsHandler))
}
