import logging
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from typing import Iterator, Dict, Any, List
from config import KafkaConfig
from health import events_consumed

logger = logging.getLogger(__name__)


class EventConsumer:

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
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000,
                max_poll_records=500,
            )
            logger.info(f"Connected to Kafka at {self.config.bootstrap_servers}, topic: {self.config.topic}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def consume(self) -> Iterator[Dict[str, Any]]:
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

    def __init__(self, input_file: str):
        self.input_file = input_file
        self.events: List[Dict[str, Any]] = []
        self._load_events()

    def _load_events(self):
        logger.info(f"Loading events from {self.input_file}")
        current_event = []

        with open(self.input_file, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('---'):
                    if current_event:
                        json_str = '\n'.join(current_event)
                        try:
                            event = json.loads(json_str)
                            self.events.append(event)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse event: {e}")
                        current_event = []
                    continue
                if ' - ' in line and (' INFO ' in line or ' ERROR ' in line or ' WARNING ' in line):
                    continue

                current_event.append(line)

            if current_event:
                json_str = '\n'.join(current_event)
                try:
                    event = json.loads(json_str)
                    self.events.append(event)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse final event: {e}")

        logger.info(f"Loaded {len(self.events)} events from file")

    def consume(self) -> Iterator[Dict[str, Any]]:
        for event in self.events:
            events_consumed.inc()
            yield event

    def commit(self):
        pass

    def close(self):
        logger.info("File consumer closed")
