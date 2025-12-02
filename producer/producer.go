package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "input-topic"
	broker := "localhost:9092"

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
		Balancer: &kafka.Hash{}, 
		Async:    false,         
	})

	data, err := json.Marshal(1)
	if err != nil {
		log.Fatal("json marshal error:", err)
	}

	err = writer.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte("task1"),
			Value: data,
		},
	)

	if err != nil {
		log.Fatal("failed to write message:", err)
	}

	fmt.Println("Message sent to input-topic")

	_ = writer.Close()
}
