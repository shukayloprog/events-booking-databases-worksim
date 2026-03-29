#!/bin/bash

echo "=========================================="
echo "🎫 EVENT BOOKING KAFKA STACK"
echo "=========================================="
echo ""

# Цвета
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Функция ожидания сервиса
wait_for_service() {
    local url=$1
    local name=$2
    local max_attempts=30
    local attempt=1
    
    echo -n "Waiting for $name"
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    echo -e " ${RED}✗${NC}"
    return 1
}

# 1. Запуск Docker Compose
echo "1. Starting Kafka stack..."
docker compose up -d

if [ $? -ne 0 ]; then
    echo -e "${RED}❌ Failed to start Docker Compose${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker Compose started${NC}"

# 2. Ожидание сервисов
echo ""
echo "2. Waiting for services to be ready..."

wait_for_service "http://localhost:8081/subjects" "Schema Registry"
wait_for_service "http://localhost:8083/connectors" "PostgreSQL Connect"
wait_for_service "http://localhost:8084/connectors" "MongoDB Connect"
wait_for_service "http://localhost:8085/connectors" "Neo4j Connect"

# 3. Создание топиков
echo ""
echo "3. Creating topics..."
./scripts/create_topics.sh

# 4. Регистрация схем
echo ""
echo "4. Registering Avro schemas..."
./scripts/register_schemas.sh

# 5. Настройка коннекторов
echo ""
echo "5. Setting up connectors..."
./scripts/setup_connectors.sh

# 6. Запуск Producer
echo ""
echo "6. Starting Producer..."
pip install -r producer/requirements.txt > /dev/null 2>&1
python3 producer/faststream_producer.py &
PRODUCER_PID=$!

# 7. Запуск Consumers
echo "7. Starting Consumers..."
pip install -r consumers/requirements.txt > /dev/null 2>&1
python3 consumers/booking_consumer.py &
BOOKING_PID=$!
python3 consumers/analytics_consumer.py &
ANALYTICS_PID=$!

# 8. Запуск Kafka Streams
echo "8. Starting Kafka Streams App..."
pip install -r streams/requirements.txt > /dev/null 2>&1
python3 streams/kafka_streams_app.py &
STREAMS_PID=$!

echo ""
echo "=========================================="
echo -e "${GREEN}✅ ALL SERVICES STARTED${NC}"
echo "=========================================="
echo ""
echo "📊 Access URLs:"
echo "  Kafka UI:      http://localhost:8080"
echo "  Schema Registry: http://localhost:8081"
echo "  PostgreSQL Connect: http://localhost:8083"
echo "  MongoDB Connect:   http://localhost:8084"
echo "  Neo4j Connect:     http://localhost:8085"
echo "  Neo4j Browser:     http://localhost:7474"
echo "  Prometheus:        http://localhost:9090"
echo "  Grafana:           http://localhost:3000 (admin/admin)"
echo ""
echo "📝 Topics:"
echo "  event-booking-events    - Raw events"
echo "  event-transformed       - Transformed events"
echo "  event-aggregated        - Aggregated metrics"
echo "  event-windowed          - Windowed calculations"
echo "  event-analytics         - Analytics output"
echo "  event-alerts            - Alert events"
echo "  event-booking-dlq       - Dead Letter Queue"
echo ""
echo "🛑 To stop all services: docker compose down"
echo ""
echo "Press Ctrl+C to stop all services..."

# Ожидание завершения
wait $PRODUCER_PID $BOOKING_PID $ANALYTICS_PID $STREAMS_PID