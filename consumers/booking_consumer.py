#!/usr/bin/env python3
"""
Consumer для обработки событий бронирований (Consumer Group 1)
Использует confluent_kafka напрямую для Avro десериализации
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ============= Конфигурация =============
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_EVENTS = "event-booking-events"
TOPIC_BOOKING_RESULTS = "booking-processing-results"
TOPIC_DLQ = "event-booking-dlq"
GROUP_ID = "booking-processor"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============= Статистика =============
stats = {
    "total_processed": 0,
    "by_type": {},
    "errors": 0,
    "dlq_sent": 0
}

# ============= Обработчики =============
def handle_booking_created(payload: Dict) -> Dict:
    booking_id = payload.get('bookingId') or payload.get('booking_id')
    user_id = payload.get('userId') or payload.get('user_id')
    quantity = payload.get('quantity', 1)
    logger.info(f"  📝 BookingCreated: {booking_id} | user={user_id} | qty={quantity}")
    return {"status": "success", "booking_id": booking_id, "action": "created"}

def handle_booking_paid(payload: Dict) -> Dict:
    booking_id = payload.get('bookingId') or payload.get('booking_id')
    amount = payload.get('totalAmount') or payload.get('total_amount', 0)
    txn = payload.get('transactionId') or payload.get('transaction_id')
    logger.info(f"  💳 BookingPaid: {booking_id} | amount={amount} | txn={txn}")
    return {"status": "success", "booking_id": booking_id, "action": "paid"}

def handle_booking_cancelled(payload: Dict) -> Dict:
    booking_id = payload.get('bookingId') or payload.get('booking_id')
    reason = payload.get('reason', 'Not specified')
    logger.info(f"  ❌ BookingCancelled: {booking_id} | reason={reason}")
    return {"status": "success", "booking_id": booking_id, "action": "cancelled"}

# ============= Producer для результатов =============
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

def send_result(event: Dict, result: Dict):
    msg = json.dumps({
        "original_event": {
            "eventId": event.get('eventId'),
            "eventType": event.get('eventType'),
            "entityId": event.get('entityId')
        },
        "result": result,
        "processed_at": datetime.now().isoformat(),
        "consumer_group": GROUP_ID
    }).encode()
    producer.produce(
        topic=TOPIC_BOOKING_RESULTS,
        value=msg,
        key=(event.get('entityId') or 'unknown').encode()
    )
    producer.poll(0)

def send_to_dlq(event: Dict, error: str):
    msg = json.dumps({
        "event": event,
        "error": error,
        "failed_at": datetime.now().isoformat(),
        "consumer_group": GROUP_ID
    }).encode()
    producer.produce(
        topic=TOPIC_DLQ,
        value=msg,
        key=(event.get('entityId') or 'unknown').encode()
    )
    producer.poll(0)
    stats["dlq_sent"] += 1
    logger.warning(f"📨 DLQ: {event.get('eventId')} | {error}")

# ============= Основной цикл =============
def main():
    # Schema Registry для Avro десериализации
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    })
    consumer.subscribe([TOPIC_EVENTS])

    print("=" * 60)
    print("📦 BOOKING CONSUMER (Group 1) STARTED")
    print("=" * 60)
    print(f"📥 Topic: {TOPIC_EVENTS}")
    print(f"📤 Result: {TOPIC_BOOKING_RESULTS}")
    print(f"⚠️  DLQ: {TOPIC_DLQ}")
    print(f"👥 Group: {GROUP_ID}")
    print("-" * 60)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                # Десериализация Avro
                event = avro_deserializer(
                    msg.value(),
                    SerializationContext(TOPIC_EVENTS, MessageField.VALUE)
                )

                if event is None:
                    continue

                event_type = event.get('eventType')
                payload = event.get('payload', {})

                logger.info(f"📥 {event_type} | entityId={event.get('entityId')}")

                if event_type == "BookingCreated":
                    result = handle_booking_created(payload)
                elif event_type == "BookingPaid":
                    result = handle_booking_paid(payload)
                elif event_type == "BookingCancelled":
                    result = handle_booking_cancelled(payload)
                else:
                    logger.info(f"⚠️ Skipped: {event_type}")
                    result = {"status": "skipped"}

                stats["total_processed"] += 1
                stats["by_type"][event_type] = stats["by_type"].get(event_type, 0) + 1

                send_result(event, result)
                logger.info(f"✅ Done: {event_type}")

            except Exception as e:
                logger.error(f"❌ Error: {e}")
                stats["errors"] += 1
                send_to_dlq({}, str(e))

    except KeyboardInterrupt:
        print("\n🛑 Stopping...")
    finally:
        consumer.close()
        producer.flush()
        print(f"\n📊 processed={stats['total_processed']} errors={stats['errors']} dlq={stats['dlq_sent']}")
        for k, v in stats["by_type"].items():
            print(f"  - {k}: {v}")

if __name__ == "__main__":
    main()