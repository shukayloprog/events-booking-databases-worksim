#!/bin/bash

echo "=== CREATING KAFKA TOPICS ==="

# Подключение к Kafka
KAFKA_CONTAINER="kafka-broker"

# Список топиков
topics=(
    "event-booking-events:3:1"
    "event-booking-dlq:1:1"
    "booking-processing-results:3:1"
    "event-analytics:3:1"
    "event-alerts:1:1"
    "event-transformed:3:1"
    "event-aggregated:3:1"
    "event-windowed:3:1"
)

for topic_config in "${topics[@]}"; do
    IFS=':' read -r topic partitions replication <<< "$topic_config"
    
    echo "Creating topic: $topic (partitions=$partitions, replication=$replication)"
    
    docker exec $KAFKA_CONTAINER kafka-topics \
        --create \
        --if-not-exists \
        --bootstrap-server localhost:9093 \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$replication"
    
    if [ $? -eq 0 ]; then
        echo "✅ Topic created: $topic"
    else
        echo "⚠️ Failed to create topic: $topic (may already exist)"
    fi
done

echo ""
echo "=== LISTING ALL TOPICS ==="
docker exec $KAFKA_CONTAINER kafka-topics \
    --list \
    --bootstrap-server localhost:9093

echo ""
echo "✅ Topic creation completed"