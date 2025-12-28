"""Main storage worker - consumes from Kafka and writes to S3."""
import asyncio
import logging
import signal
import sys
import time
from .config import Config
from .consumer import EventConsumer
from .s3_writer import S3Writer
from .health import HealthServer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StorageWorker:
    """Main storage worker service."""

    def __init__(self, config: Config):
        self.config = config
        self.consumer = EventConsumer(config.kafka)
        self.writer = S3Writer(config.s3, config.storage)
        self.health_server = HealthServer(config.health.port)
        self.running = False
        self.last_flush_time = time.time()

    async def start(self):
        """Start the worker."""
        logger.info("Starting storage worker")

        # Start health check server
        await self.health_server.start()

        # Set up signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        # Mark as ready
        self.health_server.set_ready(True)

        # Start processing
        self.running = True
        await self.process()

    async def process(self):
        """Main processing loop."""
        logger.info("Starting to consume events from Kafka")

        while self.running:
            try:
                # Consume events
                for event in self.consumer.consume():
                    # Add to buffer
                    self.writer.add_event(event)

                    # Check if should flush based on size
                    if self.writer.should_flush():
                        if self.writer.flush():
                            self.consumer.commit()
                            self.last_flush_time = time.time()

                # Check if should flush based on time
                current_time = time.time()
                time_since_flush = current_time - self.last_flush_time
                if time_since_flush >= self.config.storage.flush_interval_seconds:
                    if self.writer.get_buffer_size() > 0:
                        if self.writer.flush():
                            self.consumer.commit()
                            self.last_flush_time = current_time

                # Small sleep to prevent tight loop
                await asyncio.sleep(0.1)

            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}", exc_info=True)
                await asyncio.sleep(5)  # Back off on error

    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down storage worker")
        self.running = False

        # Flush any remaining events
        logger.info(f"Flushing {self.writer.get_buffer_size()} remaining events")
        if self.writer.flush():
            self.consumer.commit()

        # Close consumer
        self.consumer.close()

        # Stop health server
        await self.health_server.stop()

        # Exit
        sys.exit(0)


async def main():
    """Main entry point."""
    config_path = '/config/storage-worker.yaml'
    config = Config.from_file(config_path)

    worker = StorageWorker(config)
    await worker.start()


if __name__ == '__main__':
    asyncio.run(main())