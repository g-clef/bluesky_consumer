import logging
from confluent_kafka import Producer
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
            producer_config = {
                'bootstrap.servers': self.config.bootstrap_servers,
                'compression.type': self.config.compression_type,
                'batch.size': self.config.batch_size,
                'linger.ms': self.config.linger_ms,
                'acks': 'all',
                'retries': 3,
            }
            self.producer = Producer(producer_config)
            logger.info(f"Connected to Kafka at {self.config.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports."""
        if err:
            logger.error(f"Failed to deliver message: {err}")
            producer_errors.inc()
        else:
            events_produced.inc()

    def send(self, message_bytes: bytes) -> bool:
        """Send JSON event bytes to Kafka (non-blocking)."""
        try:
            self.producer.produce(
                self.config.topic,
                value=message_bytes,
                callback=self._delivery_callback
            )
            # Poll to trigger callbacks without blocking
            self.producer.poll(0)
            return True
        except BufferError:
            # Local queue is full, poll to make space
            logger.warning("Producer queue full, polling...")
            self.producer.poll(1)
            # Retry the send
            try:
                self.producer.produce(
                    self.config.topic,
                    value=message_bytes,
                    callback=self._delivery_callback
                )
                self.producer.poll(0)
                return True
            except Exception as e:
                logger.error(f"Failed to send message after retry: {e}")
                producer_errors.inc()
                return False
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            producer_errors.inc()
            return False

    def flush(self):
        if self.producer:
            self.producer.flush()

    def close(self):
        if self.producer:
            self.producer.flush()
            logger.info("Kafka producer closed")
