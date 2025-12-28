"""Kafka consumer wrapper."""
import logging
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from typing import Iterator, Dict, Any
from .config import KafkaConfig
from .health import events_consumed

logger = logging.getLogger(__name__)


class EventConsumer:
    """Kafka consumer for Bluesky events."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.consumer = None
        self._connect()

    def _connect(self):
        """Connect to Kafka."""
        try:
            self.consumer = KafkaConsumer(
                self.config.topic,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000,  # Return control periodically
                max_poll_records=500,
            )
            logger.info(f"Connected to Kafka at {self.config.bootstrap_servers}, topic: {self.config.topic}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def consume(self) -> Iterator[Dict[str, Any]]:
        """Consume events from Kafka."""
        try:
            for message in self.consumer:
                events_consumed.inc()
                yield message.value
        except KafkaError as e:
            logger.error(f"Error consuming from Kafka: {e}")
            raise

    def commit(self):
        """Commit current offsets."""
        try:
            self.consumer.commit()
        except KafkaError as e:
            logger.error(f"Error committing offsets: {e}")
            raise

    def close(self):
        """Close the consumer."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")