# Go Kafka Example

A simple demonstration of a Kafka producer and worker implemented in Go using the [Sarama](https://github.com/Shopify/sarama) library and [Fiber](https://gofiber.io/) web framework.

## Overview

This project consists of two main components:
- **Producer**: A web server that exposes an API endpoint to receive comments and push them to a Kafka topic.
- **Worker**: A consumer that listens to the Kafka topic and processes (prints) the received messages.

## Prerequisites

- [Go](https://golang.org/doc/install) (version 1.16 or later)
- [Apache Kafka](https://kafka.apache.org/quickstart) running locally on `localhost:9092`

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd go-kafka-example
   ```

2. Install dependencies:
   ```bash
   go mod download
   ```

## Running the Application

### 1. Start Kafka
Ensure your Kafka broker is running at `localhost:9092`.

### 2. Start the Worker
The worker will listen for new messages on the `comments` topic.
```bash
go run worker/worker.go
```

### 3. Start the Producer
The producer starts a web server on port `3000`.
```bash
go run producer/producer.go
```

## Testing the Flow

You can send a POST request to the producer to push a comment to Kafka:

```bash
curl -X POST http://localhost:3000/api/v1/comments \
  -H "Content-Type: application/json" \
  -d '{"text": "Hello Kafka from Go!"}'
```

### Expected Output

**Producer Logs:**
```text
Message is stored in topic(comments)/partition(0)/offset(X)
```

**Worker Logs:**
```text
Received message Count 1: | Topic(comments) | Message({"text":"Hello Kafka from Go!"})
```

## Project Structure

- `producer/`: Contains the Fiber web server and Kafka producer logic.
- `worker/`: Contains the Kafka consumer logic.
- `go.mod`: Go module definition.
