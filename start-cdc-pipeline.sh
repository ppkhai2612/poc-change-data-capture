#!/bin/bash

# Download the JDBC connector if not already present
./scripts/download-jdbc-connector.sh

# Ensure script files are executable
chmod +x ./debezium/*.sh
chmod +x ./scripts/*.sh

# Start all services...
echo "Starting all services..."
docker compose down -v
docker compose up -d --build

# Wait for services to be fully up
echo "Waiting for services to be ready..."
sleep 30

# Register the Debezium PostgreSQL connector
echo "Registering PostgreSQL CDC connector..."
./debezium/register-postgres-connector.sh

# Wait for CDC connector to initialize
echo "Waiting for CDC connector to initialize..."
sleep 15

echo "CDC pipeline is now running!"
echo "Source PostgreSQL: localhost:5432"
echo "OLAP PostgreSQL: localhost:5433"
echo "Kafka UI: http://localhost:8080"
echo "Superset UI: http://localhost:8088"
echo "Python consumer is automatically syncing data from Kafka to OLAP database"
echo ""
echo "To set up Superset visualizations automatically:"
echo "1. Install requirements: pip install -r superset/requirements.txt"
echo "2. Run the setup script: python superset/setup_superset.py"
echo ""
echo "Or to set up Superset manually:"
echo "1. Access Superset at http://localhost:8088 (username: admin, password: admin)"
echo "2. Go to Data → Databases and add connection to OLAP PostgreSQL:"
echo "   - Name: Feature Usage Analytics"
echo "   - URI: postgresql://olap_user:olap_pass@olap-postgres:5432/olap_db"
echo "3. Go to Data → Datasets and create dataset from feature_usage_counts table"
echo "4. Create bar chart visualization with feature_name as dimension and count as metric"