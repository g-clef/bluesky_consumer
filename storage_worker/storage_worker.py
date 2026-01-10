import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from typing import Dict, Any
from config import Config, S3Config, StorageConfig
from consumer import EventConsumer, FileConsumer
from s3_writer import S3Writer
from health import HealthServer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def flatten_json(nested_dict: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """
    Flatten a nested JSON dictionary.

    Args:
        nested_dict: The nested dictionary to flatten
        parent_key: The parent key prefix (used in recursion)
        sep: Separator between nested keys (default: '_')

    Returns:
        Flattened dictionary with no nested structures
    """
    items = []
    for key, value in nested_dict.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key

        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_json(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            # Flatten items in the list
            if value and isinstance(value[0], dict):
                # List of dicts - flatten each dict
                flattened_list = [flatten_json(item, sep=sep) if isinstance(item, dict) else item for item in value]
                items.append((new_key, flattened_list))
            else:
                # List of primitives - keep as-is
                items.append((new_key, value))
        else:
            # Keep primitive values as-is
            items.append((new_key, value))

    return dict(items)


class StorageWorker:
    def __init__(self, config: Config):
        self.config = config
        self.consumer = EventConsumer(config.kafka)
        self.writer = S3Writer(config.s3, config.storage)
        self.health_server = HealthServer(config.health.port)
        self.running = False
        self.last_flush_time = time.time()

    async def start(self):
        logger.info("Starting storage worker")

        await self.health_server.start()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        self.health_server.set_ready(True)

        self.running = True
        await self.process()

    async def process(self):
        logger.info("Starting to consume Jetstream JSON events from Kafka")

        while self.running:
            try:
                # Poll for a single message (non-blocking for event loop)
                message_bytes = self.consumer.poll_message(timeout=0.1)

                if message_bytes is not None:
                    try:
                        jetstream_event = json.loads(message_bytes.decode('utf-8'))

                        flattened_event = flatten_json(jetstream_event)

                        if 'time_us' in flattened_event:
                            flattened_event['event_timestamp_ms'] = flattened_event['time_us'] // 1000

                        self.writer.add_event(flattened_event)

                        if self.writer.should_flush():
                            if self.writer.flush():
                                self.consumer.commit()
                                self.last_flush_time = time.time()

                    except Exception as e:
                        logger.error(f"Error parsing/flattening message: {e}", exc_info=True)

                # Check for time-based flush
                current_time = time.time()
                time_since_flush = current_time - self.last_flush_time
                if time_since_flush >= self.config.storage.flush_interval_seconds:
                    if self.writer.get_buffer_size() > 0:
                        if self.writer.flush():
                            self.consumer.commit()
                            self.last_flush_time = current_time

                # Yield control to event loop for health checks
                await asyncio.sleep(0)

            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                break
            except Exception as e:
                logger.error(f"Error in processing loop: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def shutdown(self):
        logger.info("Shutting down storage worker")
        self.running = False

        logger.info(f"Flushing {self.writer.get_buffer_size()} remaining events")
        if self.writer.flush():
            self.consumer.commit()
        self.consumer.close()
        await self.health_server.stop()
        sys.exit(0)


class TestWorker:
    def __init__(self, input_file: str, output_dir: str, buffer_size: int = 1000):
        s3_config = S3Config(
            endpoint_url="",
            access_key_id="",
            secret_access_key="",
            bucket="",
            region=""
        )
        storage_config = StorageConfig(
            buffer_size=buffer_size,
            flush_interval_seconds=60,
            partition_format="year={year}/month={month}/day={day}/hour={hour}"
        )

        self.consumer = FileConsumer(input_file)
        self.writer = S3Writer(s3_config, storage_config, local_dir=output_dir)
        self.running = False
        self.last_flush_time = time.time()

    async def start(self):
        logger.info("Starting test storage worker")
        self.running = True
        await self.process()

    async def process(self):
        logger.info("Processing Jetstream JSON events from file")

        try:
            for message_bytes in self.consumer.consume():
                try:
                    jetstream_event = json.loads(message_bytes.decode('utf-8'))

                    # Flatten the JSON event
                    flattened_event = flatten_json(jetstream_event)

                    # Add timestamp in milliseconds for convenience
                    if 'time_us' in flattened_event:
                        flattened_event['event_timestamp_ms'] = flattened_event['time_us'] // 1000

                    self.writer.add_event(flattened_event)

                    if self.writer.should_flush():
                        self.writer.flush()

                except Exception as e:
                    logger.error(f"Error parsing/flattening message: {e}", exc_info=True)
                    continue

            if self.writer.get_buffer_size() > 0:
                logger.info(f"Flushing final {self.writer.get_buffer_size()} events")
                self.writer.flush()

            logger.info("Test worker complete!")

        except Exception as e:
            logger.error(f"Error in test processing: {e}", exc_info=True)
        finally:
            self.consumer.close()


async def test_local(input_file: str, output_dir: str, buffer_size: int = 1000):
    worker = TestWorker(input_file, output_dir, buffer_size)
    await worker.start()


async def main():
    config_path = '/config/storage-worker.yaml'
    config = Config.from_file(config_path)

    worker = StorageWorker(config)
    await worker.start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bluesky Storage Worker')
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in local test mode (reads from file, writes to local directory)'
    )
    parser.add_argument(
        '--input-file',
        type=str,
        help='Input file containing JSON-encoded Jetstream events (required for test mode)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Output directory for Parquet files (required for test mode)'
    )
    parser.add_argument(
        '--buffer-size',
        type=int,
        default=1000,
        help='Number of events per Parquet file (default: 1000)'
    )

    args = parser.parse_args()

    if args.test:
        if not args.input_file or not args.output_dir:
            parser.error("--test mode requires both --input-file and --output-dir")
        asyncio.run(test_local(args.input_file, args.output_dir, args.buffer_size))
    else:
        asyncio.run(main())
