package main

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gocql/gocql"
	"log"
	"time"
)

type CONSUMER_DATA struct {
	EventId      string
	Data         string
	EventTime    string
	ConsumedTime time.Time
	Type         string
}

type TRANSFORMED_EVENT struct {
	EventId   string
	Data      string
	EventTime string
	Type      string
}

func main() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "metadata-consumers",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	defer c.Close()

	c.SubscribeTopics([]string{"monte-carlo"}, nil)

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "events"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Failed to connect to Cassandra: %s", err)
	}
	defer session.Close()

	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}
		fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		transformedEvent := postConsumeTransformation(string(msg.Value))
		if transformedEvent.Type == "METADATA" {
			// Store metadata in Cassandra
			if err := session.Query("INSERT INTO metadata_event (EventId, Data, EventTime, ConsumedTime) VALUES (?, ?, ?, ?)", transformedEvent.EventId, transformedEvent.Data, transformedEvent.EventTime, transformedEvent.ConsumedTime).Exec(); err != nil {
				log.Fatalf("Failed to insert metadata: %s", err)
			}
		}

	}

}

func postConsumeTransformation(event string) CONSUMER_DATA {
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
	var transformedEvent TRANSFORMED_EVENT
	err := json.Unmarshal([]byte(event), &transformedEvent)
	if err != nil {
		log.Fatalf("Failed to unmarshal event: %s", err)
	}
	return CONSUMER_DATA{
		EventId:      transformedEvent.EventId,
		Data:         transformedEvent.Data,
		EventTime:    transformedEvent.EventTime,
		ConsumedTime: time.Now(),
		Type:         transformedEvent.Type,
	}

}
