#!/usr/bin/env python3
"""
Producer для системы бронирования билетов на мероприятия
Генерирует события: BookingCreated, BookingPaid, BookingCancelled
"""

import asyncio
import json
import uuid
import random
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass, asdict
from enum import Enum

from faststream import FastStream
from faststream.kafka import KafkaBroker
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ============= Конфигурация =============
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_EVENTS = "event-booking-events"
TOPIC_DLQ = "event-booking-dlq"

# ============= Типы событий =============
class EventType(str, Enum):
    BOOKING_CREATED = "BookingCreated"
    BOOKING_CONFIRMED = "BookingConfirmed"
    BOOKING_CANCELLED = "BookingCancelled"
    BOOKING_PAID = "BookingPaid"
    PAYMENT_PROCESSED = "PaymentProcessed"
    REFUND_ISSUED = "RefundIssued"
    TICKET_SOLD = "TicketSold"
    EVENT_CREATED = "EventCreated"
    USER_REGISTERED = "UserRegistered"
    REVIEW_SUBMITTED = "ReviewSubmitted"

class Source(str, Enum):
    API = "API"
    WEB = "WEB"
    MOBILE = "MOBILE"
    ADMIN = "ADMIN"
    SCHEDULER = "SCHEDULER"

# ============= Структуры данных =============
@dataclass
class Payload:
    user_id: Optional[str] = None
    event_id: Optional[str] = None
    booking_id: Optional[str] = None
    seat_ids: list = None
    ticket_type: Optional[str] = None
    quantity: Optional[int] = None
    total_amount: Optional[float] = None
    currency: Optional[str] = None
    payment_method: Optional[str] = None
    transaction_id: Optional[str] = None
    event_title: Optional[str] = None
    event_date: Optional[str] = None
    venue_id: Optional[str] = None
    venue_name: Optional[str] = None
    category: Optional[str] = None
    rating: Optional[int] = None
    comment: Optional[str] = None
    reason: Optional[str] = None
    metadata: dict = None

    def __post_init__(self):
        if self.seat_ids is None:
            self.seat_ids = []
        if self.metadata is None:
            self.metadata = {}
        # Avro map<string,string> — все значения должны быть строками
        self.metadata = {k: str(v) for k, v in self.metadata.items()}

@dataclass
class Event:
    eventId: str
    eventType: EventType
    entityId: str
    timestamp: int
    source: Source
    payload: Payload
    version: int = 1
    correlationId: Optional[str] = None
    causationId: Optional[str] = None

    def to_dict(self):
        data = asdict(self)
        data['eventType'] = self.eventType.value
        data['source'] = self.source.value
        # Convert payload keys from snake_case to camelCase to match Avro schema
        if 'payload' in data and isinstance(data['payload'], dict):
            payload = data['payload']
            mapped = {}
            snake_to_camel = {
                'user_id': 'userId',
                'event_id': 'eventId',
                'booking_id': 'bookingId',
                'seat_ids': 'seatIds',
                'ticket_type': 'ticketType',
                'total_amount': 'totalAmount',
                'payment_method': 'paymentMethod',
                'transaction_id': 'transactionId',
                'event_title': 'eventTitle',
                'event_date': 'eventDate',
                'venue_id': 'venueId',
                'venue_name': 'venueName',
            }
            for k, v in payload.items():
                mapped[snake_to_camel.get(k, k)] = v
            data['payload'] = mapped
        return data

# ============= Генератор данных =============
class EventGenerator:
    """Генератор тестовых событий для системы бронирования"""

    USERS = [
        ("user_001", "Иван Петров", "Gold"),
        ("user_002", "Мария Сидорова", "Silver"),
        ("user_003", "Алексей Смирнов", "Bronze"),
        ("user_004", "Елена Козлова", "Gold"),
        ("user_005", "Дмитрий Иванов", "Silver"),
    ]

    EVENTS = [
        ("event_001", "Рок-концерт 'Ночные снайперы'", "Концертный зал", "music", 3500),
        ("event_002", "Балет 'Лебединое озеро'", "Большой театр", "theater", 8000),
        ("event_003", "Футбольный матч: Спартак - ЦСКА", "Лужники", "sport", 2500),
        ("event_004", "Джазовый фестиваль", "Джаз-клуб", "music", 5000),
        ("event_005", "Выставка импрессионистов", "Третьяковка", "culture", 1200),
        ("event_006", "Стендап Comedy Club", "Дворец культуры", "comedy", 2000),
        ("event_007", "Классический концерт", "Филармония", "music", 4000),
        ("event_008", "Опера 'Кармен'", "Мариинский театр", "theater", 6000),
    ]

    TICKET_TYPES = ["Standard", "VIP", "Student", "Premium"]
    PAYMENT_METHODS = ["card", "online", "cash"]
    CURRENCIES = ["RUB", "USD", "EUR"]

    @classmethod
    def generate_booking_created(cls) -> Event:
        user = random.choice(cls.USERS)
        event = random.choice(cls.EVENTS)
        ticket_type = random.choice(cls.TICKET_TYPES)
        quantity = random.randint(1, 4)

        multipliers = {"Standard": 1.0, "VIP": 2.5, "Student": 0.7, "Premium": 1.8}
        price = event[4] * multipliers.get(ticket_type, 1.0)
        total = price * quantity

        payload = Payload(
            user_id=user[0],
            event_id=event[0],
            booking_id=f"booking_{uuid.uuid4().hex[:12]}",
            seat_ids=[f"{chr(65 + random.randint(0, 9))}{random.randint(1, 30)}" for _ in range(quantity)],
            ticket_type=ticket_type,
            quantity=quantity,
            total_amount=round(total, 2),
            currency=random.choice(cls.CURRENCIES),
            event_title=event[1],
            event_date=(datetime.now() + timedelta(days=random.randint(1, 60))).isoformat(),
            venue_id=f"venue_{random.randint(1, 10)}",
            venue_name=event[2],
            category=event[3],
            metadata={
                "user_agent": "Mozilla/5.0",
                "ip": f"192.168.1.{random.randint(1, 255)}",
                "device": random.choice(["desktop", "mobile", "tablet"])
            }
        )

        return Event(
            eventId=f"evt_{uuid.uuid4().hex}",
            eventType=EventType.BOOKING_CREATED,
            entityId=payload.booking_id,
            timestamp=int(datetime.now().timestamp() * 1000),
            source=random.choice(list(Source)),
            payload=payload,
            correlationId=f"corr_{uuid.uuid4().hex[:8]}"
        )

    @classmethod
    def generate_booking_paid(cls, booking_created: Optional[Event] = None) -> Event:
        if booking_created:
            payload = booking_created.payload
            return Event(
                eventId=f"evt_{uuid.uuid4().hex}",
                eventType=EventType.BOOKING_PAID,
                entityId=payload.booking_id,
                timestamp=int(datetime.now().timestamp() * 1000),
                source=Source.API,
                payload=Payload(
                    user_id=payload.user_id,
                    event_id=payload.event_id,
                    booking_id=payload.booking_id,
                    total_amount=payload.total_amount,
                    currency=payload.currency,
                    payment_method=random.choice(cls.PAYMENT_METHODS),
                    transaction_id=f"txn_{uuid.uuid4().hex[:12]}",
                    metadata={"payment_processor": random.choice(["stripe", "paypal", "yookassa"])}
                ),
                correlationId=booking_created.eventId,
                causationId=booking_created.eventId
            )

        user = random.choice(cls.USERS)
        event = random.choice(cls.EVENTS)
        ticket_type = random.choice(cls.TICKET_TYPES)
        quantity = random.randint(1, 4)
        multipliers = {"Standard": 1.0, "VIP": 2.5, "Student": 0.7, "Premium": 1.8}
        price = event[4] * multipliers.get(ticket_type, 1.0)

        return Event(
            eventId=f"evt_{uuid.uuid4().hex}",
            eventType=EventType.BOOKING_PAID,
            entityId=f"booking_{uuid.uuid4().hex[:12]}",
            timestamp=int(datetime.now().timestamp() * 1000),
            source=Source.API,
            payload=Payload(
                user_id=user[0],
                event_id=event[0],
                total_amount=round(price * quantity, 2),
                currency=random.choice(cls.CURRENCIES),
                payment_method=random.choice(cls.PAYMENT_METHODS),
                transaction_id=f"txn_{uuid.uuid4().hex[:12]}"
            )
        )

    @classmethod
    def generate_booking_cancelled(cls, booking_created: Optional[Event] = None) -> Event:
        reasons = ["Changed mind", "Wrong date", "Better price found", "Event rescheduled", "Duplicate booking"]

        if booking_created:
            payload = booking_created.payload
            return Event(
                eventId=f"evt_{uuid.uuid4().hex}",
                eventType=EventType.BOOKING_CANCELLED,
                entityId=payload.booking_id,
                timestamp=int(datetime.now().timestamp() * 1000),
                source=Source.WEB,
                payload=Payload(
                    user_id=payload.user_id,
                    event_id=payload.event_id,
                    booking_id=payload.booking_id,
                    reason=random.choice(reasons),
                    metadata={"refund_processed": str(random.choice([True, False])).lower()}
                ),
                correlationId=booking_created.eventId,
                causationId=booking_created.eventId
            )

        user = random.choice(cls.USERS)
        event = random.choice(cls.EVENTS)

        return Event(
            eventId=f"evt_{uuid.uuid4().hex}",
            eventType=EventType.BOOKING_CANCELLED,
            entityId=f"booking_{uuid.uuid4().hex[:12]}",
            timestamp=int(datetime.now().timestamp() * 1000),
            source=Source.WEB,
            payload=Payload(
                user_id=user[0],
                event_id=event[0],
                reason=random.choice(reasons)
            )
        )

    @classmethod
    def generate_event_created(cls) -> Event:
        event = random.choice(cls.EVENTS)

        return Event(
            eventId=f"evt_{uuid.uuid4().hex}",
            eventType=EventType.EVENT_CREATED,
            entityId=event[0],
            timestamp=int(datetime.now().timestamp() * 1000),
            source=Source.ADMIN,
            payload=Payload(
                event_id=event[0],
                event_title=event[1],
                event_date=(datetime.now() + timedelta(days=random.randint(7, 90))).isoformat(),
                venue_id=f"venue_{random.randint(1, 10)}",
                venue_name=event[2],
                category=event[3],
                metadata={
                    "capacity": str(random.randint(50, 1000)),
                    "price_range": f"{event[4]}-{event[4] * 3}"
                }
            )
        )

    @classmethod
    def generate_review_submitted(cls) -> Event:
        user = random.choice(cls.USERS)
        event = random.choice(cls.EVENTS)

        comments = [
            "Отличное мероприятие! Всё понравилось!",
            "Хорошо, но есть куда расти",
            "Неплохо, но дороговато",
            "Великолепно! Обязательно приду ещё",
            "Организация на высшем уровне"
        ]

        return Event(
            eventId=f"evt_{uuid.uuid4().hex}",
            eventType=EventType.REVIEW_SUBMITTED,
            entityId=event[0],
            timestamp=int(datetime.now().timestamp() * 1000),
            source=Source.MOBILE,
            payload=Payload(
                user_id=user[0],
                event_id=event[0],
                rating=random.randint(3, 5),
                comment=random.choice(comments),
                event_title=event[1]
            )
        )

# ============= Producer =============
class EventProducer:
    """Producer для отправки событий в Kafka"""

    def __init__(self):
        self.broker = KafkaBroker(
            KAFKA_BOOTSTRAP_SERVERS,
            description="Event Booking Producer"
        )
        self.app = FastStream(self.broker)
        self.schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

        with open("schemas/event.avsc", "r") as f:
            self.schema_str = f.read()

        self.avro_serializer = AvroSerializer(
            schema_str=self.schema_str,
            schema_registry_client=self.schema_registry_client
        )

    async def publish_event(self, event: Event):
        try:
            serialized = self.avro_serializer(
                event.to_dict(),
                SerializationContext(TOPIC_EVENTS, MessageField.VALUE)
            )

            await self.broker.publish(
                serialized,
                topic=TOPIC_EVENTS,
                key=event.entityId.encode()
            )

            print(f"✅ Published: {event.eventType} | entityId={event.entityId} | "
                  f"payload={event.payload.booking_id or event.payload.event_id}")

        except Exception as e:
            print(f"❌ Failed to publish event: {e}")
            await self._send_to_dlq(event, str(e))

    async def _send_to_dlq(self, event: Event, error: str):
        error_payload = {
            "original_event": event.to_dict(),
            "error": error,
            "timestamp": datetime.now().isoformat()
        }
        await self.broker.publish(
            json.dumps(error_payload).encode(),
            topic=TOPIC_DLQ,
            key=event.entityId.encode()
        )
        print(f"⚠️ Sent to DLQ: {event.eventType}")

# ============= Основной цикл =============
async def main():
    producer = EventProducer()
    generator = EventGenerator()

    await producer.broker.start()

    print("=" * 60)
    print("🎫 EVENT BOOKING PRODUCER STARTED")
    print("=" * 60)
    print(f"📤 Topic: {TOPIC_EVENTS}")
    print(f"⚠️  DLQ: {TOPIC_DLQ}")
    print("-" * 60)

    counter = 0
    last_booking = None

    try:
        while True:
            r = random.random()

            if r < 0.4:
                event = generator.generate_booking_created()
                last_booking = event
            elif r < 0.7:
                if last_booking and random.random() < 0.6:
                    event = generator.generate_booking_paid(last_booking)
                else:
                    event = generator.generate_booking_paid()
            elif r < 0.85:
                if last_booking and random.random() < 0.5:
                    event = generator.generate_booking_cancelled(last_booking)
                else:
                    event = generator.generate_booking_cancelled()
            elif r < 0.95:
                event = generator.generate_event_created()
            else:
                event = generator.generate_review_submitted()

            await producer.publish_event(event)
            counter += 1

            if counter % 10 == 0:
                print(f"\n📊 Stats: {counter} events sent")

            await asyncio.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    finally:
        await producer.broker.stop()
        print("✅ Producer stopped")

if __name__ == "__main__":
    asyncio.run(main())