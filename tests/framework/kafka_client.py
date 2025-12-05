"""Kafka client for testing."""
import logging
from typing import List
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

logger = logging.getLogger(__name__)

class KafkaTestClient:
    """Kafka operations for testing."""

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers

    def produce_messages(self, topic: str, messages: List[str]):
        """Produce messages to a topic."""
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8')
        )
        for msg in messages:
            producer.send(topic, value=msg)
        producer.flush()
        logger.info(f"Produced {len(messages)} messages to {topic}")

    def consume_messages(self, topic: str, timeout_ms: int = 10000, max_messages: int = 100) -> List[str]:
        """Consume messages from a topic."""
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: v.decode('utf-8'),
            consumer_timeout_ms=timeout_ms
        )
        messages = []
        for msg in consumer:
            messages.append(msg.value)
            if len(messages) >= max_messages:
                break
        consumer.close()
        logger.info(f"Consumed {len(messages)} messages from {topic}")
        return messages

    def create_topic(self, topic: str, partitions: int = 4, replication: int = 1):
        """Create a Kafka topic."""
        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        topic_obj = NewTopic(name=topic, num_partitions=partitions, replication_factor=replication)
        admin.create_topics([topic_obj])
        logger.info(f"Created topic {topic}")
