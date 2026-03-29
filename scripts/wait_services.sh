#!/bin/bash

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

wait_for_service() {
    local name=$1
    local container=$2
    local internal_port=$3
    local external_url=$4
    local max_attempts=${5:-80}
    local attempt=1

    echo -e "\n${CYAN}⏳ Waiting for: ${name}${NC}"
    echo -e "   Container: $container | Internal port: $internal_port"

    while [ $attempt -le $max_attempts ]; do
        # Сначала проверяем снаружи
        http_code=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 3 "$external_url" 2>/dev/null)

        if [ "$http_code" = "200" ]; then
            echo -e "   ${GREEN}✅ $name is ready! (attempt $attempt/${max_attempts}) — external OK${NC}"
            return 0
        fi

        # Если снаружи не работает — проверяем изнутри контейнера
        inner=$(docker exec "$container" curl -s -o /dev/null -w "%{http_code}" \
            --connect-timeout 3 "http://localhost:${internal_port}/connectors" 2>/dev/null)

        # Статус контейнера
        cstatus=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "no-health")

        # Прогресс-бар
        bar_filled=$((attempt * 40 / max_attempts))
        bar=$(printf '#%.0s' $(seq 1 $bar_filled))$(printf '.%.0s' $(seq $bar_filled 40))

        echo -ne "   [${attempt}/${max_attempts}] ${bar:0:40}"
        echo -ne " | ext:${YELLOW}HTTP${http_code}${NC}"
        echo -ne " | int:${CYAN}HTTP${inner}${NC}"
        echo -e  " | health:${YELLOW}${cstatus}${NC}"

        # Каждые 10 попыток — последние логи
        if [ $((attempt % 10)) -eq 0 ]; then
            echo -e "   ${YELLOW}📋 Last logs (${container}):${NC}"
            docker logs "$container" --tail 4 2>&1 | sed 's/^/      /'
        fi

        sleep 3
        attempt=$((attempt + 1))
    done

    echo -e "   ${RED}❌ $name not ready after $max_attempts attempts${NC}"
    echo -e "   ${YELLOW}Debug: docker logs $container --tail 40${NC}"
    return 1
}

echo ""
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}   KAFKA STACK — READINESS CHECK${NC}"
echo -e "${CYAN}========================================${NC}"

echo -e "\n${CYAN}📦 Container status:${NC}"
docker compose ps 2>/dev/null

echo ""

# Schema Registry — снаружи на 8081, внутри тоже 8081
wait_for_service \
    "Schema Registry" \
    "kafka-schema-registry" \
    "8081" \
    "http://localhost:8081/subjects" \
    40 || exit 1

# Postgres Connect — снаружи 8083, внутри 8083
wait_for_service \
    "Postgres Connect" \
    "kafka-connect-postgres" \
    "8083" \
    "http://localhost:8083/connectors" \
    80 || exit 1

# MongoDB Connect — снаружи 8084, внутри 8083 (!)
wait_for_service \
    "MongoDB Connect" \
    "kafka-connect-mongodb" \
    "8083" \
    "http://localhost:8084/connectors" \
    80 || exit 1

# Neo4j Connect — снаружи 8085, внутри 8083 (!)
wait_for_service \
    "Neo4j Connect" \
    "kafka-connect-neo4j" \
    "8083" \
    "http://localhost:8085/connectors" \
    80 || exit 1

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  ✅ ALL SERVICES READY${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "  ${CYAN}Kafka UI:${NC}        http://localhost:8080"
echo -e "  ${CYAN}Schema Registry:${NC} http://localhost:8081"
echo -e "  ${CYAN}PG Connect:${NC}      http://localhost:8083"
echo -e "  ${CYAN}Mongo Connect:${NC}   http://localhost:8084"
echo -e "  ${CYAN}Neo4j Connect:${NC}   http://localhost:8085"
echo ""
