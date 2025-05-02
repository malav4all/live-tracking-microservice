# Live Tracking Microservice

This is a GraphQL-based microservice for live tracking with Kafka integration. The service provides real-time data tracking through subscriptions and uses environment variables for configuration.

## Environment Variables

This service depends on the following environment variables:

| Variable | Description | Default Value | Required |
|----------|-------------|---------------|----------|
| `PORT` | The port on which the server will listen | `9090` | No |
| `KAFKA_BROKERS` | Comma-separated list of Kafka broker addresses | `localhost:9092` | Yes |
| `KAFKA_GROUP` | Kafka consumer group ID | `default_group` | No |
| `KAFKA_TOPIC` | Default Kafka topic for publishing/subscribing | `default_topic` | No |
| `RABBITMQ_URL` | RabbitMQ connection URL | `amqp://guest:guest@localhost:5672/` | No |

## Configuration

The service uses the `.env` file to load environment variables. Create a `.env` file in the root directory of the project with the following content (adjust values as needed):

```
# Server configuration
PORT=9090

# Kafka configuration
KAFKA_BROKERS=103.20.212.44:9092
KAFKA_GROUP=socket508producer
KAFKA_TOPIC=

# RabbitMQ configuration
RABBITMQ_URL=amqp://guest:guest@103.20.214.201:5655/
```

## Dependencies

This service uses the `github.com/joho/godotenv` package to load environment variables from the `.env` file. You can install it using:

```bash
go get github.com/joho/godotenv
```

## API Endpoints

- `/query` - GraphQL query endpoint
- `/playground` - GraphQL Playground UI
- `/subscriptions` - GraphQL WebSocket subscriptions endpoint

## CORS Configuration

The service is configured to allow requests from any origin (`*`) with the following HTTP methods:
- GET
- POST
- OPTIONS

And the following headers:
- Content-Type
- Authorization

## Running the Service

To run the service:

1. Make sure you have all the required dependencies installed
2. Create the `.env` file with appropriate values
3. Run the service:

```bash
go run main.go
```

## GraphQL Schema

The service provides the following GraphQL operations:

```graphql
type Query {
    hello: String!
}

type Subscription {
    track(groupId: String!, imeis: [String!], topic: String): String!
}
```

The `track` subscription allows clients to subscribe to real-time data for specific IMEIs (device identifiers) on optional topics.