#!/usr/bin/env python3
"""
Kafka Streams приложение для обработки событий
Трансформации, агрегации, оконные вычисления
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, List, Optional

from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ============= Конфигурация =============
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_EVENTS = "event-booking-events"
TOPIC_TRANSFORMED = "event-transformed"
TOPIC_AGGREGATED = "event-aggregated"
TOPIC_WINDOWED = "event-windowed"
GROUP_ID = "kafka-streams-app"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============= Утилиты =============
def ts_to_naive(ts) -> datetime:
    """Конвертация любого timestamp в naive datetime"""
    if ts is None:
        return datetime.utcnow()
    if isinstance(ts, datetime):
        if ts.tzinfo is not None:
            return ts.astimezone(timezone.utc).replace(tzinfo=None)
        return ts
    try:
        return datetime.utcfromtimestamp(int(ts) / 1000)
    except Exception:
        return datetime.utcnow()


def safe_json(obj):
    """JSON сериализатор с поддержкой datetime и set"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Not serializable: {type(obj)}")


def wrap_with_schema(payload: Dict) -> Dict:
    """Wrap a payload dict with JSON Schema envelope for Kafka Connect JsonConverter"""
    fields = []
    for key, value in payload.items():
        if isinstance(value, bool):
            field_type = "boolean"
        elif isinstance(value, int):
            field_type = "int64"
        elif isinstance(value, float):
            field_type = "double"
        else:
            field_type = "string"
        fields.append({"field": key, "type": field_type, "optional": True})
    # Convert all values to strings for non-primitive types
    clean_payload = {}
    for key, value in payload.items():
        if value is None:
            clean_payload[key] = None
        elif isinstance(value, (bool, int, float)):
            clean_payload[key] = value
        else:
            clean_payload[key] = str(value)
    return {
        "schema": {"type": "struct", "fields": fields, "optional": False},
        "payload": clean_payload
    }


def create_topics():
    admin = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    topics = [
        NewTopic(TOPIC_TRANSFORMED, num_partitions=3, replication_factor=1),
        NewTopic(TOPIC_AGGREGATED, num_partitions=3, replication_factor=1),
        NewTopic(TOPIC_WINDOWED, num_partitions=3, replication_factor=1)
    ]
    try:
        fs = admin.create_topics(topics)
        for topic, f in fs.items():
            try:
                f.result()
                logger.info(f"✅ Topic created: {topic}")
            except Exception as e:
                logger.info(f"Topic may already exist: {topic} - {e}")
    except Exception as e:
        logger.error(f"Failed to create topics: {e}")


# ============= 1. Трансформация =============
def transform_event(event: Dict) -> Dict:
    event_type = event.get('eventType')
    payload = event.get('payload') or {}

    transformed = {
        "original_event_id": event.get('eventId'),
        "event_type": event_type,
        "entity_id": event.get('entityId'),
        "timestamp": ts_to_naive(event.get('timestamp')).isoformat(),
        "source": event.get('source'),
        "version": event.get('version', 1),
        "transformed_at": datetime.now().isoformat(),
    }

    if event_type == "BookingCreated":
        amount = float(payload.get('totalAmount') or payload.get('total_amount') or 0)
        currency = payload.get('currency') or 'RUB'
        ticket_type = payload.get('ticketType') or payload.get('ticket_type') or ''
        transformed.update({
            "booking_id": payload.get('bookingId') or payload.get('booking_id'),
            "user_id": payload.get('userId') or payload.get('user_id'),
            "event_id": payload.get('eventId') or payload.get('event_id'),
            "event_title": payload.get('eventTitle') or payload.get('event_title'),
            "ticket_type": ticket_type,
            "quantity": int(payload.get('quantity') or 0),
            "total_amount": amount,
            "currency": currency,
            "category": payload.get('category'),
            "venue_name": payload.get('venueName') or payload.get('venue_name'),
            "amount_in_rub": _convert_to_rub(amount, currency),
            "ticket_type_multiplier": _get_ticket_multiplier(ticket_type),
            "is_premium": ticket_type in ['VIP', 'Premium']
        })

    elif event_type == "BookingPaid":
        amount = float(payload.get('totalAmount') or payload.get('total_amount') or 0)
        currency = payload.get('currency') or 'RUB'
        transformed.update({
            "booking_id": payload.get('bookingId') or payload.get('booking_id'),
            "user_id": payload.get('userId') or payload.get('user_id'),
            "event_id": payload.get('eventId') or payload.get('event_id'),
            "payment_method": payload.get('paymentMethod') or payload.get('payment_method'),
            "transaction_id": payload.get('transactionId') or payload.get('transaction_id'),
            "amount": amount,
            "currency": currency,
            "amount_in_rub": _convert_to_rub(amount, currency),
            "payment_processed": True
        })

    elif event_type == "BookingCancelled":
        transformed.update({
            "booking_id": payload.get('bookingId') or payload.get('booking_id'),
            "user_id": payload.get('userId') or payload.get('user_id'),
            "event_id": payload.get('eventId') or payload.get('event_id'),
            "reason": payload.get('reason'),
            "refund_processed": str(payload.get('metadata', {}).get('refund_processed', 'false'))
        })

    elif event_type == "ReviewSubmitted":
        transformed.update({
            "user_id": payload.get('userId') or payload.get('user_id'),
            "event_id": payload.get('eventId') or payload.get('event_id'),
            "event_title": payload.get('eventTitle') or payload.get('event_title'),
            "rating": int(payload.get('rating') or 0),
            "comment": payload.get('comment'),
            "sentiment": _analyze_sentiment(payload.get('comment') or '')
        })

    elif event_type == "EventCreated":
        transformed.update({
            "event_id": payload.get('eventId') or payload.get('event_id'),
            "event_title": payload.get('eventTitle') or payload.get('event_title'),
            "category": payload.get('category'),
            "venue_name": payload.get('venueName') or payload.get('venue_name'),
        })

    return transformed


def _convert_to_rub(amount, currency: str) -> float:
    rates = {'RUB': 1.0, 'USD': 95.0, 'EUR': 103.0}
    try:
        return float(amount or 0) * rates.get(currency or 'RUB', 1.0)
    except (TypeError, ValueError):
        return 0.0


def _get_ticket_multiplier(ticket_type: str) -> float:
    return {'Student': 0.7, 'Standard': 1.0, 'Premium': 1.8, 'VIP': 2.5}.get(ticket_type or '', 1.0)


def _analyze_sentiment(text: str) -> str:
    text_lower = (text or '').lower()
    if any(w in text_lower for w in ['отлично', 'хорошо', 'великолепно', 'понравилось', 'рекомендую']):
        return 'positive'
    if any(w in text_lower for w in ['плохо', 'ужасно', 'разочарован', 'не понравилось']):
        return 'negative'
    return 'neutral'


# ============= 2. Агрегация =============
class EventAggregator:
    def __init__(self):
        self.event_agg: Dict[str, Dict] = defaultdict(lambda: {
            "total_bookings": 0, "total_revenue": 0.0, "total_tickets": 0,
            "unique_users": set(), "cancellations": 0,
            "avg_rating": 0.0, "ratings_sum": 0, "ratings_count": 0
        })
        self.user_agg: Dict[str, Dict] = defaultdict(lambda: {
            "total_bookings": 0, "total_spent": 0.0,
            "events_attended": set(), "favorite_categories": defaultdict(int),
            "cancellations": 0
        })
        self.cat_agg: Dict[str, Dict] = defaultdict(lambda: {
            "total_bookings": 0, "total_revenue": 0.0,
            "total_tickets": 0, "events_count": set(), "avg_rating": 0.0
        })

    def aggregate(self, ev: Dict) -> Dict:
        et = ev.get('event_type')
        if et == "BookingCreated":
            self._booking(ev)
        elif et == "BookingPaid":
            self._payment(ev)
        elif et == "ReviewSubmitted":
            self._review(ev)
        elif et == "BookingCancelled":
            self._cancellation(ev)

        return {
            "timestamp": datetime.now().isoformat(),
            "event_type": et,
            "entity_id": ev.get('entity_id'),
            "aggregates": {
                "event": self._get_event(ev.get('event_id')),
                "user": self._get_user(ev.get('user_id')),
                "category": self._get_cat(ev.get('category'))
            }
        }

    def _booking(self, ev):
        eid, uid, cat = ev.get('event_id'), ev.get('user_id'), ev.get('category')
        qty = int(ev.get('quantity') or 1)
        if eid:
            self.event_agg[eid]['total_bookings'] += 1
            self.event_agg[eid]['total_tickets'] += qty
            if uid:
                self.event_agg[eid]['unique_users'].add(uid)
        if uid:
            self.user_agg[uid]['total_bookings'] += 1
            if eid:
                self.user_agg[uid]['events_attended'].add(eid)
            if cat:
                self.user_agg[uid]['favorite_categories'][cat] += 1
        if cat:
            self.cat_agg[cat]['total_bookings'] += 1
            self.cat_agg[cat]['total_tickets'] += qty
            if eid:
                self.cat_agg[cat]['events_count'].add(eid)

    def _payment(self, ev):
        eid, uid, cat = ev.get('event_id'), ev.get('user_id'), ev.get('category')
        amount = float(ev.get('amount_in_rub') or 0)
        if eid:
            self.event_agg[eid]['total_revenue'] += amount
        if uid:
            self.user_agg[uid]['total_spent'] += amount
        if cat:
            self.cat_agg[cat]['total_revenue'] += amount

    def _review(self, ev):
        eid, rating = ev.get('event_id'), int(ev.get('rating') or 0)
        if eid and eid in self.event_agg:
            a = self.event_agg[eid]
            a['ratings_sum'] += rating
            a['ratings_count'] += 1
            a['avg_rating'] = a['ratings_sum'] / a['ratings_count']

    def _cancellation(self, ev):
        eid, uid = ev.get('event_id'), ev.get('user_id')
        if eid:
            self.event_agg[eid]['cancellations'] += 1
        if uid:
            self.user_agg[uid]['cancellations'] += 1

    def _get_event(self, eid) -> Dict:
        if not eid or eid not in self.event_agg:
            return {}
        a = self.event_agg[eid]
        return {
            "total_bookings": a['total_bookings'],
            "total_revenue": round(a['total_revenue'], 2),
            "total_tickets": a['total_tickets'],
            "unique_users": len(a['unique_users']),
            "cancellations": a['cancellations'],
            "avg_rating": round(a['avg_rating'], 2),
        }

    def _get_user(self, uid) -> Dict:
        if not uid or uid not in self.user_agg:
            return {}
        a = self.user_agg[uid]
        return {
            "total_bookings": a['total_bookings'],
            "total_spent": round(a['total_spent'], 2),
            "events_attended": len(a['events_attended']),
            "favorite_category": max(a['favorite_categories'].items(), key=lambda x: x[1])[0] if a['favorite_categories'] else None,
            "cancellations": a['cancellations']
        }

    def _get_cat(self, cat) -> Dict:
        if not cat or cat not in self.cat_agg:
            return {}
        a = self.cat_agg[cat]
        return {
            "total_bookings": a['total_bookings'],
            "total_revenue": round(a['total_revenue'], 2),
            "total_tickets": a['total_tickets'],
            "events_count": len(a['events_count']),
            "avg_rating": round(a['avg_rating'], 2)
        }


# ============= 3. Оконные вычисления =============
class WindowedCalculator:
    def __init__(self, window_minutes: int = 60):
        self.window_minutes = window_minutes
        self.windows: Dict[str, Dict] = {}
        self.completed_windows: List[Dict] = []

    def process(self, event: Dict, event_time: datetime) -> Optional[Dict]:
        # Всегда naive
        if event_time.tzinfo is not None:
            event_time = event_time.astimezone(timezone.utc).replace(tzinfo=None)

        window_key = event_time.strftime('%Y-%m-%d_%H:00')

        if window_key not in self.windows:
            ws = event_time.replace(minute=0, second=0, microsecond=0)
            self.windows[window_key] = {
                "window_start": ws,
                "window_end": ws + timedelta(minutes=self.window_minutes),
                "events_count": 0,
                "metrics": {
                    "total_events": 0,
                    "by_type": defaultdict(int),
                    "total_revenue": 0.0,
                    "total_bookings": 0,
                    "unique_users": set(),
                    "unique_events": set(),
                    "top_categories": defaultdict(int)
                }
            }

        w = self.windows[window_key]
        w['events_count'] += 1
        et = event.get('event_type', 'unknown')
        w['metrics']['total_events'] += 1
        w['metrics']['by_type'][et] += 1

        if et == 'BookingPaid':
            w['metrics']['total_revenue'] += float(event.get('amount_in_rub') or 0)
        if et == 'BookingCreated':
            w['metrics']['total_bookings'] += 1
            if event.get('user_id'):
                w['metrics']['unique_users'].add(event['user_id'])
            if event.get('event_id'):
                w['metrics']['unique_events'].add(event['event_id'])
            if event.get('category'):
                w['metrics']['top_categories'][event['category']] += 1

        # Проверка завершения окна (naive)
        if datetime.utcnow() >= w['window_end']:
            return self._finalize(window_key)
        return None

    def _finalize(self, window_key: str) -> Optional[Dict]:
        if window_key not in self.windows:
            return None
        w = self.windows.pop(window_key)
        top_cats = w['metrics']['top_categories']
        result = {
            "window_type": f"tumbling_{self.window_minutes}m",
            "window_start": w['window_start'].isoformat(),
            "window_end": w['window_end'].isoformat(),
            "metrics": {
                "total_events": w['metrics']['total_events'],
                "by_type": dict(w['metrics']['by_type']),
                "total_revenue": round(w['metrics']['total_revenue'], 2),
                "total_bookings": w['metrics']['total_bookings'],
                "unique_users": len(w['metrics']['unique_users']),
                "unique_events": len(w['metrics']['unique_events']),
                "top_category": max(top_cats.items(), key=lambda x: x[1])[0] if top_cats else None,
                "avg_booking_value": round(
                    w['metrics']['total_revenue'] / max(1, w['metrics']['total_bookings']), 2
                )
            },
            "event_count": w['events_count']
        }
        self.completed_windows.append(result)
        if len(self.completed_windows) > 100:
            self.completed_windows.pop(0)
        return result


# ============= Основное приложение =============
class KafkaStreamsApp:
    def __init__(self):
        schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self._avro_deserializer = AvroDeserializer(schema_registry_client)

        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        })
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.aggregator = EventAggregator()
        self.window_calculator = WindowedCalculator(window_minutes=60)
        self.consumer.subscribe([TOPIC_EVENTS])
        logger.info("🚀 Kafka Streams App initialized")

    def delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")

    def process_message(self, msg):
        try:
            event = self._avro_deserializer(
                msg.value(),
                SerializationContext(TOPIC_EVENTS, MessageField.VALUE)
            )
            if event is None:
                return

            event_time = ts_to_naive(event.get('timestamp'))
            logger.info(f"📥 {event.get('eventType')} | {event.get('entityId')}")

            # 1. Трансформация
            transformed = transform_event(event)
            wrapped = wrap_with_schema(transformed)
            self.producer.produce(
                TOPIC_TRANSFORMED,
                json.dumps(wrapped, default=safe_json).encode(),
                key=(event.get('entityId') or '').encode(),
                callback=self.delivery_report
            )

            # 2. Агрегация
            aggregated = self.aggregator.aggregate(transformed)
            self.producer.produce(
                TOPIC_AGGREGATED,
                json.dumps(aggregated, default=safe_json).encode(),
                key=(event.get('entityId') or '').encode(),
                callback=self.delivery_report
            )

            # 3. Оконное вычисление
            windowed = self.window_calculator.process(transformed, event_time)
            if windowed:
                self.producer.produce(
                    TOPIC_WINDOWED,
                    json.dumps(windowed, default=safe_json).encode(),
                    key=windowed['window_start'].encode(),
                    callback=self.delivery_report
                )
                logger.info(f"📊 Window: {windowed['window_start']} → {windowed['window_end']}")

            self.producer.poll(0)
            logger.info(f"✅ Done: {event.get('eventType')}")

        except Exception as e:
            logger.error(f"❌ Error: {e}", exc_info=True)

    def run(self):
        logger.info("🔄 Starting Kafka Streams processing...")
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    break
                self.process_message(msg)
        except KeyboardInterrupt:
            logger.info("🛑 Stopping...")
        finally:
            self.consumer.close()
            self.producer.flush()
            logger.info("✅ Kafka Streams app stopped")


def main():
    create_topics()
    KafkaStreamsApp().run()


if __name__ == "__main__":
    main()