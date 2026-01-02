package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
)

const (
	BrokerUrl = "localhost:9092"
	TopicName = "comments"
)

// Comment struct
type Comment struct {
	Text string `form:"text" json:"text"`
}

// Handler holds dependencies for API handlers
type Handler struct {
	producer sarama.SyncProducer
}

func main() {
	// Connect to Kafka Producer once
	producer, err := ConnectProducer([]string{BrokerUrl})
	if err != nil {
		log.Fatalf("Failed to connect to Kafka producer: %v", err)
	}
	// Ensure producer is closed on exit
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer: %v", err)
		}
	}()

	// Create Handler instance with the shared producer
	h := &Handler{producer: producer}

	app := fiber.New()
	api := app.Group("/api/v1")

	// Use the handler method
	api.Post("/comments", h.createComment)

	// Run server in a separate goroutine to allow signal handling
	go func() {
		if err := app.Listen(":3000"); err != nil {
			log.Printf("Server listening error: %v", err)
		}
	}()

	fmt.Println("Server started on :3000")

	// Graceful Shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-
c // Block until signal received
	fmt.Println("Shutting down server...")

	if err := app.Shutdown(); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// createComment handler
func (h *Handler) createComment(c *fiber.Ctx) error {
	// Instantiate new Comment struct
	cmt := new(Comment)

	// Parse body into comment struct
	if err := c.BodyParser(cmt); err != nil {
		log.Println(err)
		return c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": err.Error(),
		})
	}

	// Convert body into bytes
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		return c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error processing comment data",
		})
	}

	// Create Kafka message
	msg := &sarama.ProducerMessage{
		Topic: TopicName,
		Value: sarama.StringEncoder(cmtInBytes),
	}

	// Send message using the shared producer
	partition, offset, err := h.producer.SendMessage(msg)
	if err != nil {
		log.Printf("Error sending message to Kafka: %v", err)
		return c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error pushing to queue",
		})
	}

	fmt.Printf("Message stored in topic(%s)/partition(%d)/offset(%d)\n", TopicName, partition, offset)

	// Return success response
	return c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
}
