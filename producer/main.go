package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/lib/pq"
)

type TransformedEvent struct {
	EventId   string
	Data      string
	EventTime string
	Type      string
	Table     string
}

type SchemaChangeLog struct {
	TableName     string    `json:"table_name"`
	OperationType string    `json:"operation_type"`
	ChangedAt     time.Time `json:"changed_at"`
}

type Column struct {
	ColumnName string `json:"column_name"`
	DataType   string `json:"data_type"`
}

const (
	connStr = "user=rudder dbname=sources sslmode=disable port=7432 password=password"
)

var (
	kafkaMonteCarloMessagesProduced = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_monte_carlo_messages_produced_total",
	})
	postgresSchemaChanges = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "postgres_schema_changes_total",
	})
	kafkaProduceDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_produce_duration_seconds",
		Buckets: prometheus.DefBuckets,
	})
)

func init() {
	// Register the metrics with Prometheus
	prometheus.MustRegister(kafkaMonteCarloMessagesProduced)
	prometheus.MustRegister(postgresSchemaChanges)
	prometheus.MustRegister(kafkaProduceDuration)
}
func main() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(":9091", nil))
	}()
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}

	defer p.Close()

	go MonteCarloEvents(p)
	go PostgresTriggerEvents(p)
	// Wait for message deliveries
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
			}
		}
	}

}

func PostgresTriggerEvents(producer *kafka.Producer) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}
	defer db.Close()

	// Periodically check for new schema changes
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	var lastFetched time.Time
	for {
		select {
		case <-ticker.C:
			schemaChanges, err := fetchSchemaChanges(db, lastFetched.UTC())
			fmt.Println(lastFetched)
			fmt.Println(schemaChanges)
			if err != nil {
				log.Printf("Error fetching schema changes: %v", err)
				continue
			}

			for _, change := range schemaChanges {
				fmt.Printf("Change: %v\n", change)
				if columns, err := handleSchemaChange(db, change); err != nil {
					log.Printf("Error processing schema change: %v", err)
				} else {
					fmt.Printf("Columns: %s\n", columns)
					transformedColumns := preIngestTransformation(transformColumns(columns), change.OperationType, change.TableName)
					bytes, err := json.Marshal(transformedColumns)
					if err != nil {
						log.Printf("Failed to marshal event: %s", err)
						continue
					}

					// send these columns to kafka producer
					topic := "internal"
					start := time.Now()
					producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
						Value:          bytes,
					}, nil)

					postgresSchemaChanges.Inc()
					kafkaProduceDuration.Observe(time.Since(start).Seconds())
				}
			}
			lastFetched = time.Now()
		}
	}
}

func transformColumns(columns []Column) string {
	// Create a string in the format "name type, name type, ..."
	var transformedColumns []string
	for _, column := range columns {
		transformedColumns = append(transformedColumns, column.ColumnName+" "+column.DataType)
	}
	return strings.Join(transformedColumns, ", ")
}

func MonteCarloEvents(producer *kafka.Producer) {
	numGoroutines := 5
	// Start multiple goroutines to simulate incoming metadata from Monte Carlo
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for {
				actualEvent := fmt.Sprintf("metadata-event-%d-from-goroutine-%d", time.Now().Unix(), goroutineID)
				transformedEvent := preIngestTransformation(actualEvent, "METADATA", "")
				bytes, err := json.Marshal(transformedEvent)
				if err != nil {
					log.Printf("Failed to marshal event: %s", err)
					continue
				}

				topic := "monte-carlo"
				start := time.Now()
				producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          bytes,
				}, nil)
				fmt.Println("incrementing")
				kafkaMonteCarloMessagesProduced.Inc()
				kafkaProduceDuration.Observe(time.Since(start).Seconds())
			}
		}(i)
	}
}

func preIngestTransformation(event string, eventType string, table string) TransformedEvent {
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
	return TransformedEvent{
		EventId:   uniqueId,
		Data:      event,
		EventTime: eventTime,
		Type:      eventType,
		Table:     table,
	}
}

func fetchSchemaChanges(db *sql.DB, lastFetched time.Time) ([]SchemaChangeLog, error) {
	query := `
		SELECT table_name, operation_type, changed_at
		FROM schema_change
		WHERE changed_at > $1
		ORDER BY changed_at ASC;
	`

	rows, err := db.Query(query, lastFetched)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var changes []SchemaChangeLog
	for rows.Next() {
		var change SchemaChangeLog
		if err := rows.Scan(&change.TableName, &change.OperationType, &change.ChangedAt); err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}
	return changes, nil
}

func handleSchemaChange(db *sql.DB, change SchemaChangeLog) (columns []Column, err error) {
	query := `SELECT column_name, data_type
	FROM information_schema.columns
	WHERE table_schema = $1 AND table_name = $2;`
	// separate tableName by dot
	tableName := strings.Split(change.TableName, ".")
	rows, err := db.Query(query, tableName[0], tableName[1])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var column Column
		if err := rows.Scan(&column.ColumnName, &column.DataType); err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, nil
}
