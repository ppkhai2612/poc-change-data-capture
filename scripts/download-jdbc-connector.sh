#!/bin/bash

JDBC_CONNECTOR_VERSION=10.7.4
DOWNLOAD_DIR=./jdbc-connector

mkdir -p $DOWNLOAD_DIR

if [ ! -f $DOWNLOAD_DIR/kafka-connect-jdbc-$JDBC_CONNECTOR_VERSION.jar ]; then
    echo "Downloading Kafka Connect JDBC Connector..."
    curl -L https://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/$JDBC_CONNECTOR_VERSION/kafka-connect-jdbc-$JDBC_CONNECTOR_VERSION.jar -o $DOWNLOAD_DIR/kafka-connect-jdbc-$JDBC_CONNECTOR_VERSION.jar
    
    # Also download the PostgreSQL JDBC driver
    curl -L https://jdbc.postgresql.org/download/postgresql-42.5.4.jar -o $DOWNLOAD_DIR/postgresql-42.5.4.jar
    
    echo "Download complete!"
else
    echo "JDBC Connector already downloaded!"
fi

chmod 755 $DOWNLOAD_DIR/*.jar