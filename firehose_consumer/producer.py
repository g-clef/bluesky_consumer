import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import KafkaConfig
from health import producer_errors, events_produced

logger = logging.getLogger(__name__)


class EventProducer:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer = None
        self._connect()

    def _connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                compression_type=self.config.compression_type,
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                acks='all',
                retries=3,
            )
            logger.info(f"Connected to Kafka at {self.config.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def send(self, message_bytes: bytes) -> bool:
        """Send JSON event bytes to Kafka."""
        try:
            future = self.producer.send(self.config.topic, value=message_bytes)
            future.get(timeout=10)
            events_produced.inc()
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            producer_errors.inc()
            return False

    def flush(self):
        if self.producer:
            self.producer.flush()

    def close(self):
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")
