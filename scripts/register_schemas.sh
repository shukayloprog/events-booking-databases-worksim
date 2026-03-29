#!/bin/bash

echo "=== REGISTERING AVRO SCHEMAS ==="

SCHEMA_REGISTRY_URL="http://localhost:8081"
SCHEMA_FILE="schemas/event.avsc"

# Проверка существования файла
if [ ! -f "$SCHEMA_FILE" ]; then
    echo "❌ Schema file not found: $SCHEMA_FILE"
    exit 1
fi

# Регистрация схемы
echo "Registering schema for topic: event-booking-events"

curl -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"schema\": $(jq -c . $SCHEMA_FILE)}" \
    "$SCHEMA_REGISTRY_URL/subjects/event-booking-events-value/versions"

if [ $? -eq 0 ]; then
    echo "✅ Schema registered successfully"
else
    echo "❌ Failed to register schema"
fi

echo ""
echo "=== LISTING ALL SCHEMAS ==="
curl -s "$SCHEMA_REGISTRY_URL/subjects" | jq .

echo ""
echo "✅ Schema registration completed"