import json
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

class Bus:
    def __init__(self, brokers: str):
        self._brokers = brokers
        self.producer: AIOKafkaProducer | None = None
        self.consumer: AIOKafkaConsumer | None = None

    async def start_producer(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self._brokers,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: (k or "").encode()
        )
        await self.producer.start()

    async def start_consumer(self, topic: str, group_id: str):
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._brokers,
            group_id=group_id,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode()),
            key_deserializer=lambda k: k.decode() if k else None
        )
        await self.consumer.start()

    async def publish(self, topic: str, key: str | None, value: dict):
        assert self.producer, "producer not started"
        await self.producer.send_and_wait(topic, key=key, value=value)

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()
