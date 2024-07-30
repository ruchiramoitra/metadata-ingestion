package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

type TRANSFORMED_EVENT struct {
	EventId   string
	Data      string
	EventTime string
	Type      string
}

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	defer p.Close()

	numGoroutines := 5

	// Start multiple goroutines to simulate incoming metadata from Monte Carlo
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for {
				actualEvent := fmt.Sprintf("metadata-event-%d-from-goroutine-%d", time.Now().Unix(), goroutineID)
				transformedEvent := preIngestTransformation(actualEvent)
				bytes, err := json.Marshal(transformedEvent)
				if err != nil {
					log.Printf("Failed to marshal event: %s", err)
					continue
				}

				topic := "monte-carlo"
				p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          bytes,
				}, nil)
			}
		}(i)
	}

	// Wait for message deliveries
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
			} else {
				fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
			}
		}
	}
}

func preIngestTransformation(event string) TRANSFORMED_EVENT {
	// let's say we want to convert the event into a json in the following format which is expected by the consumer
	/**
	{
		"event": "METADATA-EVENT-<TIMESTAMP>-FROM-GOROUTINE-<GOROUTINE-ID>",
	"event--unique-id": "EVENT-UNIQUE-ID",
	"event-time": "<EVENT-TIME>"
	"type": "METADATA"
	}
	*/
	// We can achieve this by splitting the event string and formatting it into the desired format

	uniqueId := fmt.Sprintf("EVENT-UNIQUE-ID-%d", time.Now().UnixNano())
	eventTime := time.Now().Format(time.RFC3339)
	return TRANSFORMED_EVENT{
		EventId:   uniqueId,
		Data:      event,
		EventTime: eventTime,
		Type:      "METADATA",
	}
}
