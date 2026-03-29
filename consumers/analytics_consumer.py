#!/usr/bin/env python3
"""
Consumer для аналитики (Consumer Group 2)
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Any

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ============= Конфигурация =============
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_EVENTS = "event-booking-events"
TOPIC_ANALYTICS = "event-analytics"
TOPIC_ALERTS = "event-alerts"
GROUP_ID = "analytics-processor"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============= Producer =============
producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

def publish(topic: str, data: Dict, key: str = "analytics"):
    producer.produce(
        topic=topic,
        value=json.dumps(data).encode(),
        key=key.encode()
    )
    producer.poll(0)

# ============= Аналитика =============
class AnalyticsProcessor:
    def __init__(self):
        self.event_metrics = {}      # event_id -> metrics
        self.category_stats = defaultdict(lambda: {"bookings": 0, "revenue": 0.0, "tickets": 0})
        self.hourly_stats = {}       # hour -> stats
        self.recent_events = []      # оконное окно
        self.window_hours = 24
        logger.info("📊 Analytics Processor initialized")

    def process(self, event: Dict):
        event_type = event.get('eventType')
        payload = event.get('payload', {})
        ts = event.get('timestamp', datetime.now())
        if isinstance(ts, datetime):
            event_time = ts
        else:
            event_time = datetime.fromtimestamp(ts / 1000)

        if event_type == "BookingCreated":
            self._booking_created(payload, event_time)
        elif event_type == "BookingPaid":
            self._booking_paid(payload, event_time)
        elif event_type == "BookingCancelled":
            self._booking_cancelled(payload)
        elif event_type == "EventCreated":
            self._event_created(payload)

        # Оконное окно
        self.recent_events.append({'event': event, 'time': event_time, 'type': event_type})
        cutoff = event_time - timedelta(hours=self.window_hours)
        self.recent_events = [e for e in self.recent_events if e['time'] > cutoff]

        # Проверка алертов
        self._check_alerts(event_time)

        # Отправка метрик
        self._send_metrics(event_time)

    def _booking_created(self, payload: Dict, event_time: datetime):
        event_id = payload.get('event_id') or payload.get('eventId')
        if not event_id:
            return
        if event_id not in self.event_metrics:
            self.event_metrics[event_id] = {
                "event_id": event_id,
                "title": payload.get('event_title') or payload.get('eventTitle', ''),
                "bookings": 0, "revenue": 0.0, "tickets": 0,
                "cancellations": 0, "users": set()
            }
        m = self.event_metrics[event_id]
        m["bookings"] += 1
        m["tickets"] += payload.get('quantity') or 1
        user = payload.get('user_id') or payload.get('userId')
        if user:
            m["users"].add(user)

        cat = payload.get('category', 'unknown')
        self.category_stats[cat]["bookings"] += 1
        self.category_stats[cat]["tickets"] += payload.get('quantity') or 1

        hour = event_time.strftime('%Y-%m-%d %H:00')
        if hour not in self.hourly_stats:
            self.hourly_stats[hour] = {"bookings": 0, "revenue": 0.0}
        self.hourly_stats[hour]["bookings"] += 1

    def _booking_paid(self, payload: Dict, event_time: datetime):
        event_id = payload.get('event_id') or payload.get('eventId')
        amount = payload.get('total_amount') or payload.get('totalAmount') or 0
        if event_id and event_id in self.event_metrics:
            self.event_metrics[event_id]["revenue"] += amount
        cat = payload.get('category', 'unknown')
        self.category_stats[cat]["revenue"] += amount
        hour = event_time.strftime('%Y-%m-%d %H:00')
        if hour in self.hourly_stats:
            self.hourly_stats[hour]["revenue"] += amount

    def _booking_cancelled(self, payload: Dict):
        event_id = payload.get('event_id') or payload.get('eventId')
        if event_id and event_id in self.event_metrics:
            self.event_metrics[event_id]["cancellations"] += 1

    def _event_created(self, payload: Dict):
        event_id = payload.get('event_id') or payload.get('eventId')
        if event_id and event_id not in self.event_metrics:
            self.event_metrics[event_id] = {
                "event_id": event_id,
                "title": payload.get('event_title') or payload.get('eventTitle', ''),
                "bookings": 0, "revenue": 0.0, "tickets": 0,
                "cancellations": 0, "users": set()
            }

    def _check_alerts(self, current_time: datetime):
        # Алерт: много отмен за последний час
        cancellations = sum(
            1 for e in self.recent_events
            if e['type'] == 'BookingCancelled'
            and e['time'] > current_time - timedelta(hours=1)
        )
        if cancellations > 10:
            alert = {
                "type": "HIGH_CANCELLATION_RATE",
                "severity": "WARNING",
                "message": f"{cancellations} cancellations in last hour",
                "timestamp": current_time.isoformat()
            }
            publish(TOPIC_ALERTS, alert, "HIGH_CANCELLATION_RATE")
            logger.warning(f"🚨 ALERT: {alert['message']}")

    def _send_metrics(self, current_time: datetime):
        # Топ-5 мероприятий по выручке
        top_events = sorted(
            [
                {
                    "event_id": m["event_id"],
                    "title": m["title"],
                    "revenue": m["revenue"],
                    "bookings": m["bookings"],
                    "tickets": m["tickets"],
                    "unique_users": len(m["users"])
                }
                for m in self.event_metrics.values()
            ],
            key=lambda x: x["revenue"],
            reverse=True
        )[:5]

        # Последние 12 часов
        hourly = sorted(self.hourly_stats.items(), reverse=True)[:12]

        metrics = {
            "timestamp": current_time.isoformat(),
            "window_hours": self.window_hours,
            "events_in_window": len(self.recent_events),
            "top_events": top_events,
            "category_stats": dict(self.category_stats),
            "hourly_stats": [{"hour": h, **s} for h, s in hourly],
        }
        publish(TOPIC_ANALYTICS, metrics)

# ============= Основной цикл =============
def main():
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client)

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    })
    consumer.subscribe([TOPIC_EVENTS])

    processor = AnalyticsProcessor()

    print("=" * 60)
    print("📈 ANALYTICS CONSUMER (Group 2) STARTED")
    print("=" * 60)
    print(f"📥 Topic: {TOPIC_EVENTS}")
    print(f"📤 Analytics: {TOPIC_ANALYTICS}")
    print(f"🚨 Alerts: {TOPIC_ALERTS}")
    print(f"👥 Group: {GROUP_ID}")
    print("-" * 60)

    processed = 0
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
                event = avro_deserializer(
                    msg.value(),
                    SerializationContext(TOPIC_EVENTS, MessageField.VALUE)
                )
                if event is None:
                    continue

                processor.process(event)
                processed += 1

                if processed % 10 == 0:
                    logger.info(f"📊 Processed {processed} events | window={len(processor.recent_events)}")

            except Exception as e:
                logger.error(f"❌ Error: {e}")

    except KeyboardInterrupt:
        print(f"\n🛑 Stopping... processed={processed}")
    finally:
        consumer.close()
        producer.flush()

if __name__ == "__main__":
    main()