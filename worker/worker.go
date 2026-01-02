package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
)

const (
	BrokerUrl = "localhost:9092"
	TopicName = "comments"
)

func main() {
	worker, err := connectConsumer([]string{BrokerUrl})
	if err != nil {
		panic(err)
	}
	// Ensure worker connection closes on exit
	defer func() {
		if err := worker.Close(); err != nil {
			log.Printf("Failed to close worker: %v", err)
		}
	}()

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(TopicName, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	// Ensure consumer partition closes on exit
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Failed to close consumer partition: %v", err)
		}
	}()

	fmt.Println("Consumer started")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})
	
go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interrupt detected")
			doneCh <- struct{}{}
				return
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
