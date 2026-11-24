import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

import psycopg2
from kafka import KafkaConsumer
from pythonjsonlogger import jsonlogger

# Configure logging
log_handler = RotatingFileHandler('feature_counts_consumer.log', maxBytes=10485760, backupCount=5)
log_formatter = jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(name)s %(message)s')
log_handler.setFormatter(log_formatter)

logger = logging.getLogger("feature_counts_consumer")
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

# Also log to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# Global flag for graceful shutdown
running = True

# Graceful shutdown handler
def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received, finishing current work...")
    running = False

# Register shutdown handlers
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def connect_to_olap_db():
    """Connect to the OLAP PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("OLAP_DB_HOST", "olap-postgres"),
            port=os.getenv("OLAP_DB_PORT", "5432"),
            database=os.getenv("OLAP_DB_NAME", "olap_db"),
            user=os.getenv("OLAP_DB_USER", "olap_user"),
            password=os.getenv("OLAP_DB_PASSWORD", "olap_pass")
        )
        conn.autocommit = False  # Use transactions for safety
        logger.info("Connected to OLAP database successfully")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to OLAP database: {str(e)}")
        sys.exit(1)


def create_kafka_consumer():
    """Create and return a Kafka consumer for the feature_usage_counts topic"""
    try:
        consumer = KafkaConsumer(
            "feature_usage_counts",
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            group_id="feature-counts-consumer-group",
            auto_offset_reset="earliest",  # Start from the beginning of the topic if no offset is stored
            enable_auto_commit=False,      # Don't auto-commit offsets
            value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None
        )
        logger.info("Kafka consumer created successfully")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer: {str(e)}")
        sys.exit(1)


def update_feature_count(conn, feature_name, count):
    """Update the feature count in the OLAP database"""
    try:
        cursor = conn.cursor()
        
        # Use an upsert (INSERT ... ON CONFLICT ... DO UPDATE) operation
        query = """
        INSERT INTO feature_usage_counts (feature_name, count)
        VALUES (%s, %s)
        ON CONFLICT (feature_name)
        DO UPDATE SET count = %s;
        """
        
        cursor.execute(query, (feature_name, count, count))
        conn.commit()
        cursor.close()
        
        logger.info(f"Updated count for feature '{feature_name}' to {count}")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating feature count: {str(e)}")
        return False


def process_messages(consumer, conn):
    """Process messages from the Kafka topic and update the OLAP database"""
    try:
        # Poll for messages with a timeout (in milliseconds)
        messages = consumer.poll(timeout_ms=1000, max_records=100)
        
        if not messages:
            return 0  # No messages received
        
        processed_count = 0
        
        # Process received messages
        for topic_partition, partition_messages in messages.items():
            for message in partition_messages:
                try:
                    # Extract feature_name (key) and count (value)
                    feature_name = message.key.decode('utf-8') if message.key else None
                    
                    # The value should be a JSON object with a "count" field
                    if not message.value or "count" not in message.value:
                        logger.warning(f"Skipping message with invalid value format: {message.value}")
                        continue
                    
                    count = message.value["count"]
                    
                    # Update the feature count in the OLAP database
                    if feature_name and update_feature_count(conn, feature_name, count):
                        processed_count += 1
                        
                        # Manually commit offset after successful processing
                        consumer.commit({topic_partition: message.offset + 1})
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    # Don't commit offset for failed messages
        
        return processed_count
    except Exception as e:
        logger.error(f"Error polling messages: {str(e)}")
        return 0


def main():
    """Main function to run the consumer"""
    logger.info("Starting feature counts consumer...")
    
    # Connect to the OLAP database
    conn = connect_to_olap_db()
    
    # Create Kafka consumer
    consumer = create_kafka_consumer()
    
    total_processed = 0
    last_log_time = time.time()
    
    try:
        # Process messages until shutdown signal is received
        while running:
            # Process messages from the Kafka topic
            processed_count = process_messages(consumer, conn)
            total_processed += processed_count
            
            # Log processing metrics periodically
            current_time = time.time()
            if processed_count > 0 or (current_time - last_log_time) >= 60:
                logger.info(f"Processed {processed_count} messages (total: {total_processed})")
                last_log_time = current_time
            
            # Sleep briefly to avoid CPU spinning if no messages
            if processed_count == 0:
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        # Clean up resources
        try:
            consumer.close()
            logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {str(e)}")
        
        try:
            conn.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")
        
        logger.info(f"Shutdown complete. Total processed: {total_processed}")


if __name__ == "__main__":
    main()