import argparse
import asyncio
import json
import logging
import signal
import sys
import time
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message
from config import Config
from producer import EventProducer
from metadata_extractor import MetadataExtractor
from health import HealthServer, events_received, connection_status, reconnections, processing_lag_seconds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FirehoseConsumer:
    def __init__(self, config: Config):
        self.config = config
        self.producer = EventProducer(config.kafka)
        self.extractor = MetadataExtractor()
        self.health_server = HealthServer(config.health.port)
        self.running = False
        self.reconnect_delay = config.firehose.reconnect_delay

    async def start(self):
        logger.info("Starting firehose consumer")
        await self.health_server.start()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        self.running = True
        await self.consume()

    def on_message_handler(self, message):
        try:
            events_received.inc()
            commit = parse_subscribe_repos_message(message)
            events = self.extractor.extract(commit)
            if events:
                # Calculate processing lag from first event's timestamp
                current_time_ms = time.time() * 1000
                event_time_ms = events[0].get('event_timestamp', current_time_ms)
                lag_seconds = (current_time_ms - event_time_ms) / 1000.0
                processing_lag_seconds.set(lag_seconds)

                for event in events:
                    self.producer.send(event)

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    async def consume(self):
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


async def test_local(max_events: int = 10):
    logger.info(f"Starting local test mode - will consume {max_events} events")

    extractor = MetadataExtractor()
    event_count = 0

    def on_message(message):
        nonlocal event_count
        try:
            commit = parse_subscribe_repos_message(message)
            events = extractor.extract(commit)

            if events:
                for event in events:
                    event_count += 1
                    print(f"\n--- Event {event_count} ---")
                    print(json.dumps(event, indent=2, default=str))

                    if event_count >= max_events:
                        logger.info(f"Reached {max_events} events, stopping...")
                        client.stop()
                        return

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    try:
        logger.info("Connecting to Bluesky firehose...")
        client = FirehoseSubscribeReposClient()
        logger.info("Connected. Consuming events...")
        client.start(on_message)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        logger.info(f"Total events consumed: {event_count}")


async def main():
    config_path = '/config/firehose-consumer.yaml'
    config = Config.from_file(config_path)

    consumer = FirehoseConsumer(config)
    await consumer.start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bluesky Firehose Consumer')
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in local test mode (no Kafka, prints to console)'
    )
    parser.add_argument(
        '--max-events',
        type=int,
        default=10,
        help='Maximum number of events to consume in test mode (default: 10)'
    )

    args = parser.parse_args()

    if args.test:
        asyncio.run(test_local(args.max_events))
    else:
        asyncio.run(main())
