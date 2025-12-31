import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from typing import Iterator, List
from config import KafkaConfig
from health import events_consumed

logger = logging.getLogger(__name__)


class EventConsumer:
    """Kafka consumer for raw firehose messages."""

    def __init__(self, config: KafkaConfig):
        self.config = config
        self.consumer = None
        self._connect()

    def _connect(self):
        try:
            self.consumer = KafkaConsumer(
                self.config.topic,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                # No deserializer - we receive raw bytes
                consumer_timeout_ms=1000,
                max_poll_records=500,
            )
            logger.info(f"Connected to Kafka at {self.config.bootstrap_servers}, topic: {self.config.topic}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def consume(self) -> Iterator[bytes]:
        """Consume raw message bytes from Kafka."""
        try:
            for message in self.consumer:
                events_consumed.inc()
                yield message.value
        except KafkaError as e:
            logger.error(f"Error consuming from Kafka: {e}")
            raise

    def commit(self):
        try:
            self.consumer.commit()
        except KafkaError as e:
            logger.error(f"Error committing offsets: {e}")
            raise

    def close(self):
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


class FileConsumer:
    """File-based consumer for testing - reads JSON messages from a file."""

    def __init__(self, input_file: str):
        self.input_file = input_file
        self.messages: List[bytes] = []
        self._load_messages()

    def _load_messages(self):
        """Load JSON-encoded Jetstream events from file (one per line)."""
        logger.info(f"Loading JSON-encoded Jetstream events from {self.input_file}")

        with open(self.input_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    # Validate it's valid JSON, then store as UTF-8 bytes
                    json.loads(line)  # Validate
                    self.messages.append(line.encode('utf-8'))
                except Exception as e:
                    logger.warning(f"Failed to parse JSON line: {e}")
                    continue

        logger.info(f"Loaded {len(self.messages)} Jetstream events from file")

    def consume(self) -> Iterator[bytes]:
        """Yield raw message bytes."""
        for message_bytes in self.messages:
            events_consumed.inc()
            yield message_bytes

    def commit(self):
        pass

    def close(self):
        logger.info("File consumer closed")
