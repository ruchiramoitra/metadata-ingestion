Metadata Ingestion Service
This project provides a metadata ingestion service and schema change trigger from postgres that consumes events through Kafka, processes the events, and stores them in Cassandra databases. It is designed to handle various types of metadata events, transform them as needed, and support schema changes.

Features
Kafka Integration: Consumes events from Kafka topics.
Cassandra Integration: Stores transformed metadata events.
PostgreSQL Integration: Tracks schema changes and updates tables accordingly.
Event Transformation: Transforms raw events into a structured format.
Technologies Used
Go: Main programming language.
Kafka: For messaging and event streaming.
Cassandra: For storing metadata events.
PostgreSQL: For tracking schema changes.
Docker: For running kafka 

For now postgres and cassandra is running locally.
