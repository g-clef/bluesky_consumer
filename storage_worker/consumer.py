import json
import logging
from confluent_kafka import Consumer, KafkaException, KafkaError as ConfluentKafkaError
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
            consumer_config = {
                'bootstrap.servers': self.config.bootstrap_servers,
                'group.id': self.config.group_id,
                'auto.offset.reset': self.config.auto_offset_reset,
                'enable.auto.commit': self.config.enable_auto_commit,
                'max.poll.interval.ms': 300000,
            }
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe([self.config.topic])
            logger.info(f"Connected to Kafka at {self.config.bootstrap_servers}, topic: {self.config.topic}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def poll_message(self, timeout: float = 1.0):
        """Poll for a single message from Kafka. Returns None if no message available."""
        try:
            msg = self.consumer.poll(timeout=timeout)
            if msg is None:
                return None
            if msg.error():
                if msg.error().code() == ConfluentKafkaError._PARTITION_EOF:
                    return None
                else:
                    raise KafkaException(msg.error())

            events_consumed.inc()
            return msg.value()
        except KafkaException as e:
            logger.error(f"Error consuming from Kafka: {e}")
            raise

    def commit(self):
        try:
            self.consumer.commit(asynchronous=False)
        except Exception as e:
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
                    json.loads(line)
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
