# Metadata Ingestion Service

This project provides a metadata ingestion service and schema change trigger from postgres that consumes events through Kafka, processes the events, and stores them in Cassandra databases. It is designed to handle various types of metadata events, transform them as needed, and support schema changes.

# Features
## Kafka Integration: Consumes events from Kafka topics.
## Cassandra Integration: Stores transformed metadata events.
## PostgreSQL Integration: Tracks schema changes and updates tables accordingly.
## Event Transformation: Transforms raw events into a structured format.
## Metrics and Visualisation: Collect number of events being produced and the duration to produce by kafka and visualise in dashboard for analytics.
## Containerized: App can be run by running docker compose up. All the features will be running as mentioned above.

# Technologies Used
Go: Main programming language.

Kafka: For messaging and event streaming.

Cassandra: For storing metadata events.

PostgreSQL: For tracking schema changes.

Docker: For running kafka 

Prometheus: For collecting metrics

Grafana: For visualising metrics


Script to tirgger changes in schema:

`CREATE TABLE schema_change (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

select * from schema_change;
CREATE OR REPLACE FUNCTION log_schema_change()
RETURNS event_trigger AS $$
DECLARE
    cmd RECORD;
BEGIN
    -- Loop through all commands that triggered the event
    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        INSERT INTO schema_change (table_name, operation_type)
        VALUES (cmd.object_identity, cmd.command_tag);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER schema_change_trigger
ON ddl_command_end
WHEN TAG = ('ALTER TABLE', 'CREATE TABLE', 'DROP TABLE')
EXECUTE PROCEDURE log_schema_change();`
