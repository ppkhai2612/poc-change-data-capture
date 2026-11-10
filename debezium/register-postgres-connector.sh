#!/bin/bash

# This script registers a Debezium PostgreSQL connector with Kafka Connect.
# Purpose: Sets up Change Data Capture (CDC) from PostgreSQL to Kafka
# The connector monitors changes in the specified PostgreSQL tables and
# streams these changes as events to Apache Kafka topics.

curl -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d '{
      "name": "postgres-connector",
      "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "saas_db",
        "database.server.name": "saas",
        "topic.prefix": "saas",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.name": "debezium_pub",
        "table.include.list": "public.users,public.feature_usage",
        "snapshot.mode": "initial"
      }
}'