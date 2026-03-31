import asyncio
import json

from aiokafka import AIOKafkaConsumer
from loguru import logger

from app.config import settings
from app.socket.registry import registry
from app.socket.server import sio

MAX_RETRIES = 10
INITIAL_BACKOFF = 2


class KafkaConsumerService:
    def __init__(self) -> None:
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._tasks.append(
            asyncio.create_task(
                self._consume_topic(
                    settings.KAFKA_TOPIC_RIDE_UPDATES, self._handle_ride_update
                )
            )
        )
        self._tasks.append(
            asyncio.create_task(
                self._consume_topic(
                    settings.KAFKA_TOPIC_DRIVER_ASSIGNED, self._handle_driver_assigned
                )
            )
        )
        self._tasks.append(
            asyncio.create_task(
                self._consume_topic(
                    settings.KAFKA_TOPIC_DRIVER_LOCATION_TO_PASSENGER,
                    self._handle_driver_location_to_passenger,
                )
            )
        )
        self._tasks.append(
            asyncio.create_task(
                self._consume_topic(
                    settings.KAFKA_TOPIC_RIDE_NOTIFICATIONS,
                    self._handle_ride_notification,
                )
            )
        )
        logger.info("Kafka consumers started")

    async def _consume_topic(self, topic: str, handler) -> None:
        backoff = INITIAL_BACKOFF
        for attempt in range(1, MAX_RETRIES + 1):
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=settings.KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            try:
                await consumer.start()
                logger.info("Consumer connected to topic '{}'", topic)
                backoff = INITIAL_BACKOFF
                async for msg in consumer:
                    try:
                        await handler(msg.value)
                    except Exception:
                        logger.exception("Error handling message from {}", topic)
            except asyncio.CancelledError:
                await consumer.stop()
                return
            except Exception as e:
                logger.warning(
                    "Consumer for '{}' failed (attempt {}/{}): {}",
                    topic, attempt, MAX_RETRIES, e,
                )
                try:
                    await consumer.stop()
                except Exception:
                    pass
                if attempt == MAX_RETRIES:
                    logger.error("Consumer for '{}' gave up after {} attempts", topic, MAX_RETRIES)
                    return
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _handle_ride_update(self, data: dict) -> None:
        ride_id = data.get("ride_id")
        event_name = data.get("event", "ride_update")
        if ride_id:
            await sio.emit(event_name, data, room=str(ride_id))

    async def _handle_driver_assigned(self, data: dict) -> None:
        passenger_id = data.get("passenger_id")
        ride_id = str(data.get("ride_id", ""))

        sid = registry.get_sid(str(passenger_id))
        if sid:
            await sio.emit("driver_assigned", data, to=sid)
            await sio.enter_room(sid, ride_id)

        driver_id = data.get("driver_id")
        if driver_id:
            driver_sid = registry.get_sid(str(driver_id))
            if driver_sid:
                await sio.emit("driver_assigned", data, to=driver_sid)
                await sio.enter_room(driver_sid, ride_id)

    async def _handle_driver_location_to_passenger(self, data: dict) -> None:
        passenger_id = data.get("passenger_id")
        if passenger_id:
            sid = registry.get_sid(str(passenger_id))
            if sid:
                await sio.emit("driver_location", data, to=sid)

    async def _handle_ride_notification(self, data: dict) -> None:
        user_id = data.get("user_id")
        event_name = data.get("event", "notification")
        if user_id:
            sid = registry.get_sid(str(user_id))
            if sid:
                await sio.emit(event_name, data, to=sid)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        logger.info("Kafka consumers stopped")


kafka_consumer = KafkaConsumerService()
