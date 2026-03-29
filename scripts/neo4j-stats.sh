#!/bin/bash
CYPHER="docker exec neo4j cypher-shell -u neo4j -p admin123"

echo "===== NEO4J STATS ====="
echo ""

echo "--- Nodes & Relationships ---"
$CYPHER "MATCH (n) RETURN labels(n)[0] AS type, count(n) AS count;"
echo ""
$CYPHER "MATCH ()-[r]->() RETURN type(r) AS rel_type, count(r) AS count;"

echo ""
echo "--- Top Users by Activity ---"
$CYPHER "MATCH (u:User)-[r:INTERACTED]->(e:Event) RETURN u.id AS user, count(r) AS interactions, collect(DISTINCT e.title) AS events ORDER BY interactions DESC LIMIT 5;"

echo ""
echo "--- Events by Popularity ---"
$CYPHER "MATCH (u:User)-[r:INTERACTED]->(e:Event) RETURN e.title AS event, e.category AS category, count(r) AS interactions, count(DISTINCT u) AS unique_users ORDER BY interactions DESC;"

echo ""
echo "--- Interaction Types ---"
$CYPHER "MATCH ()-[r:INTERACTED]->() RETURN r.type AS event_type, count(r) AS count ORDER BY count DESC;"

echo ""
echo "--- Recent Bookings (last 10) ---"
$CYPHER "MATCH (u:User)-[r:INTERACTED]->(e:Event) WHERE r.type = 'BookingCreated' RETURN u.id AS user, e.title AS event, r.booking_id AS booking, r.quantity AS qty, r.amount AS amount ORDER BY r.timestamp DESC LIMIT 10;"
