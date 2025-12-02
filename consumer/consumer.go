package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := "localhost:9092"
	inputTopic := "input-topic"
	outputTopic := "output-topic"

	// Global context + graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// ---------- Readers ----------
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       inputTopic,
		GroupID:     "service-In",
		MinBytes:    1,
		MaxBytes:    10e6,
		CommitInterval: 1 * time.Second,
	})
	defer reader.Close()

	readerOut := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       outputTopic,
		GroupID:     "service-Out",
		StartOffset: kafka.LastOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer readerOut.Close()

	// ---------- Writer ----------
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{broker},
		Topic:        outputTopic,
		Balancer:     &kafka.Hash{},  
	})
	defer writer.Close()

	fmt.Println("Consumer service started...")

	// ---------- INPUT PIPELINE ----------
	go func() {
		defer recoverGoroutine()

		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil { return }
				log.Println("input read err:", err)
				continue
			}

			log.Printf("[INPUT] key=%s value=%s\n", msg.Key, msg.Value)

			// Process
			var data int
			if err := json.Unmarshal(msg.Value, &data); err != nil {
				log.Println("Invalid JSON:", err)
				continue
			}

			data++

			outBytes, _ := json.Marshal(data)

			// Write response
			if err := writer.WriteMessages(ctx, kafka.Message{
				Key:   msg.Key,
				Value: outBytes,
			}); err != nil {
				log.Println("write err:", err)
				continue
			}

			log.Println("[OUTPUT WRITE] Success")
		}
	}()

	// ---------- OUTPUT PIPELINE ----------
	go func() {
		defer recoverGoroutine()

		for {
			msgOut, err := readerOut.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil { return }
				log.Println("output read err:", err)
				continue
			}

			log.Printf("[OUTPUT READ] key=%s value=%s\n", msgOut.Key, msgOut.Value)
		}
	}()

	<-ctx.Done()
	fmt.Println("Stopped.")
}

// Prevent panic from killing goroutine
func recoverGoroutine() {
	if r := recover(); r != nil {
		log.Println("goroutine panic:", r)
	}
}
