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
                'queue.buffering.max.messages': 500000,
                'queue.buffering.max.kbytes': 1048576,  # 1GB buffer
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

    def send(self, message_bytes: bytes, max_retries: int = 10) -> bool:
        """Send JSON event bytes to Kafka with backpressure handling.

        Uses exponential backoff when the producer queue is full,
        polling to drain the queue before retrying.
        """
        for attempt in range(max_retries + 1):
            try:
                self.producer.produce(
                    self.config.topic,
                    value=message_bytes,
                    callback=self._delivery_callback
                )
                self.producer.poll(0)
                return True
            except BufferError:
                if attempt < max_retries:
                    # Exponential backoff: 0.1s, 0.2s, 0.4s, 0.8s, etc., capped at 5s
                    wait_time = min(0.1 * (2 ** attempt), 5.0)
                    logger.warning(
                        f"Producer queue full, polling for {wait_time:.1f}s "
                        f"(attempt {attempt + 1}/{max_retries + 1})"
                    )
                    self.producer.poll(wait_time)
                else:
                    logger.error("Producer queue full after max retries, dropping message")
                    producer_errors.inc()
                    return False
            except Exception as e:
                logger.error(f"Failed to send message to Kafka: {e}")
                producer_errors.inc()
                return False
        return False

    def flush(self):
        if self.producer:
            self.producer.flush()

    def close(self):
        if self.producer:
            self.producer.flush()
            logger.info("Kafka producer closed")
