"""Main firehose consumer - connects to Bluesky and produces to Kafka."""
import asyncio
import logging
import signal
import sys
import time
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message, CAR
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

        # Start health check server
        await self.health_server.start()

        # Set up signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        # Start consuming
        self.running = True
        await self.consume()

    async def consume(self):
        """Main consumption loop with reconnection logic."""
        while self.running:
            try:
                logger.info(f"Connecting to firehose at {self.config.firehose.endpoint}")
                connection_status.set(0)

                # Create firehose client
                client = FirehoseSubscribeReposClient()

                # Define message handler
                def on_message_handler(message):
                    """Handle incoming firehose message."""
                    try:
                        events_received.inc()

                        # Parse the message
                        commit = parse_subscribe_repos_message(message)

                        # Convert to dict for processing
                        if hasattr(commit, 'model_dump'):
                            message_dict = commit.model_dump()
                        else:
                            message_dict = {'raw': message}

                        # Add message type
                        if hasattr(commit, '__class__'):
                            message_dict['t'] = f"#{commit.__class__.__name__.lower()}"

                        # Extract metadata
                        event = self.extractor.extract(message_dict)
                        if event:
                            # Send to Kafka
                            self.producer.send(event)

                    except Exception as e:
                        logger.error(f"Error processing message: {e}", exc_info=True)

                # Start the client
                connection_status.set(1)
                self.health_server.set_ready(True)
                logger.info("Connected to firehose, consuming events...")

                client.start(on_message_handler)

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

                    # Exponential backoff
                    self.reconnect_delay = min(
                        self.reconnect_delay * 2,
                        self.config.firehose.max_reconnect_delay
                    )
                else:
                    break

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down firehose consumer")
        self.running = False

        # Flush any pending messages
        self.producer.flush()
        self.producer.close()

        # Stop health server
        await self.health_server.stop()

        # Exit
        sys.exit(0)


async def main():
    """Main entry point."""
    config_path = '/config/firehose-consumer.yaml'
    config = Config.from_file(config_path)

    consumer = FirehoseConsumer(config)
    await consumer.start()


if __name__ == '__main__':
    asyncio.run(main())