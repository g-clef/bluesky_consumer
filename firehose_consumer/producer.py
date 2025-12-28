"""Kafka producer wrapper."""
import logging
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
from typing import Dict, Any
from .config import KafkaConfig
from .health import producer_errors, events_produced

logger = logging.getLogger(__name__)


class EventProducer:
    """Kafka producer for Bluesky events."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer = None
        self._connect()

    def _connect(self):
        """Connect to Kafka."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                compression_type=self.config.compression_type,
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                acks='all',  # Wait for all replicas
                retries=3,
            )
            logger.info(f"Connected to Kafka at {self.config.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def send(self, event: Dict[str, Any]) -> bool:
        """Send event to Kafka topic."""
        try:
            future = self.producer.send(self.config.topic, value=event)
            # Wait for send to complete
            future.get(timeout=10)
            events_produced.inc()
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event to Kafka: {e}")
            producer_errors.inc()
            return False

    def flush(self):
        """Flush any pending messages."""
        if self.producer:
            self.producer.flush()

    def close(self):
        """Close the producer."""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")