"""Main firehose consumer - connects to Bluesky and produces to Kafka."""
import asyncio
import logging
import signal
import sys
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message
from .config import Config
from .producer import EventProducer
from .metadata_extractor import MetadataExtractor
from .health import HealthServer, events_received, connection_status, reconnections

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FirehoseConsumer:
    """Main firehose consumer service."""

    def __init__(self, config: Config):
        self.config = config
        self.producer = EventProducer(config.kafka)
        self.extractor = MetadataExtractor()
        self.health_server = HealthServer(config.health.port)
        self.running = False
        self.reconnect_delay = config.firehose.reconnect_delay

    async def start(self):
        """Start the consumer."""
        logger.info("Starting firehose consumer")
        await self.health_server.start()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        self.running = True
        await self.consume()

    def on_message_handler(self, message):
        """Handle incoming firehose message."""
        try:
            events_received.inc()
            commit = parse_subscribe_repos_message(message)
            events = self.extractor.extract(commit)
            if events:
                for event in events:
                    self.producer.send(event)

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    async def consume(self):
        """Main consumption loop with reconnection logic."""
        while self.running:
            try:
                logger.info(f"Connecting to firehose at {self.config.firehose.endpoint}")
                connection_status.set(0)
                client = FirehoseSubscribeReposClient()
                connection_status.set(1)
                self.health_server.set_ready(True)
                logger.info("Connected to firehose, consuming events...")
                client.start(self.on_message_handler)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                break
            except Exception as e:
                logger.error(f"Firehose connection error: {e}", exc_info=True)
                connection_status.set(0)
                self.health_server.set_ready(False)
                reconnections.inc()

                if self.running:
                    logger.info(f"Reconnecting in {self.reconnect_delay} seconds...")
                    await asyncio.sleep(self.reconnect_delay)

                    self.reconnect_delay = min(
                        self.reconnect_delay * 2,
                        self.config.firehose.max_reconnect_delay
                    )
                else:
                    break

    async def shutdown(self):
        logger.info("Shutting down firehose consumer")
        self.running = False
        self.producer.flush()
        self.producer.close()
        await self.health_server.stop()
        sys.exit(0)


async def main():
    """Main entry point."""
    config_path = '/config/firehose-consumer.yaml'
    config = Config.from_file(config_path)

    consumer = FirehoseConsumer(config)
    await consumer.start()


if __name__ == '__main__':
    asyncio.run(main())
