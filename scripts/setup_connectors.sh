#!/bin/bash

echo "=== SETTING UP KAFKA CONNECTORS ==="

# Функция для создания коннектора
create_connector() {
    local connector_name=$1
    local connector_config=$2
    local connect_url=$3
    
    echo ""
    echo "Creating connector: $connector_name"
    
    # Удаление существующего коннектора
    curl -X DELETE "$connect_url/connectors/$connector_name" 2>/dev/null
    
    # Создание нового коннектора
    response=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        --data @"$connector_config" \
        "$connect_url/connectors")
    
    if echo "$response" | grep -q "created"; then
        echo "✅ Connector created: $connector_name"
    else
        echo "⚠️ Response: $response"
    fi
}

# PostgreSQL коннектор
echo ""
echo "=== POSTGRES CONNECTOR ==="
create_connector "postgres-event-sink" "connect/postgres-sink.json" "http://localhost:8083"

# MongoDB коннектор
echo ""
echo "=== MONGODB CONNECTOR ==="
create_connector "mongodb-event-sink" "connect/mongodb-sink.json" "http://localhost:8084"

# Neo4j коннектор
echo ""
echo "=== NEO4J CONNECTOR ==="
create_connector "neo4j-event-sink" "connect/neo4j-sink.json" "http://localhost:8085"

echo ""
echo "=== LISTING ALL CONNECTORS ==="
echo ""
echo "PostgreSQL Connect:"
curl -s "http://localhost:8083/connectors" | jq .
echo ""
echo "MongoDB Connect:"
curl -s "http://localhost:8084/connectors" | jq .
echo ""
echo "Neo4j Connect:"
curl -s "http://localhost:8085/connectors" | jq .

echo ""
echo "✅ Connectors setup completed"