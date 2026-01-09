import argparse
import asyncio
import json
import logging
import signal
import sys
import time
import websockets
from config import Config
from producer import EventProducer
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
        self.health_server = HealthServer(config.health.port)
        self.running = False
        self.reconnect_delay = config.firehose.reconnect_delay

    def _build_endpoint_url(self) -> str:
        """Build the full endpoint URL with wanted collections query parameters."""
        base_url = self.config.firehose.endpoint
        if self.config.firehose.wanted_collections:
            collections = self.config.firehose.wanted_collections.split(',')
            query_params = '&'.join([f'wantedCollections={c.strip()}' for c in collections])
            return f'{base_url}?{query_params}'
        return base_url

    async def start(self):
        logger.info("Starting Jetstream consumer")
        await self.health_server.start()
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
        self.running = True
        await self.consume()

    def send_event(self, event):
        self.producer.send(event)

    async def consume(self):
        while self.running:
            try:
                endpoint_url = self._build_endpoint_url()
                logger.info(f"Connecting to Jetstream at {endpoint_url}")
                connection_status.set(0)

                async with websockets.connect(
                    endpoint_url,
                    max_size=10 * 1024 * 1024,  # 10MB max message size
                    ping_interval=20,
                    ping_timeout=10,
                ) as websocket:
                    connection_status.set(1)
                    self.health_server.set_ready(True)
                    logger.info("Connected to Jetstream, consuming events...")

                    self.reconnect_delay = self.config.firehose.reconnect_delay

                    async for message in websocket:
                        if not self.running:
                            break
                        try:
                            events_received.inc()
                            event = json.loads(message)

                            # Calculate processing lag
                            if 'time_us' in event:
                                event_time_us = event['time_us']
                                current_time_us = time.time() * 1_000_000  # Convert to microseconds
                                lag_seconds = (current_time_us - event_time_us) / 1_000_000
                                processing_lag_seconds.set(lag_seconds)

                            self.producer.send(message.encode('utf-8') if isinstance(message, str) else message)

                        except Exception as e:
                            logger.error(f"Error processing message: {e}", exc_info=True)
                            continue

            except KeyboardInterrupt:
                logger.info("Received interrupt signal")
                break
            except Exception as e:
                logger.error(f"Jetstream connection error: {e}", exc_info=True)
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
        logger.info("Shutting down Jetstream consumer")
        self.running = False
        self.producer.flush()
        self.producer.close()
        await self.health_server.stop()
        sys.exit(0)


async def test_local(max_events: int = 10, output_file: str = None, endpoint: str = None):
    logger.info(f"Starting local test mode - will consume {max_events} events")

    if not endpoint:
        endpoint = "wss://jetstream2.us-east.bsky.network/subscribe"

    if output_file:
        logger.info(f"Writing JSON events to {output_file}")
        output = open(output_file, 'w')
    else:
        output = None

    event_count = 0
    websocket = None

    try:
        logger.info(f"Connecting to Jetstream at {endpoint}...")
        async with websockets.connect(
            endpoint,
            max_size=10 * 1024 * 1024,
            ping_interval=20,
            ping_timeout=10,
        ) as websocket:
            logger.info("Connected. Consuming events...")

            async for message in websocket:
                try:
                    event_count += 1
                    if output:
                        output.write(message + '\n')
                    else:
                        print(message)
                    if event_count >= max_events:
                        logger.info(f"Reached {max_events} events, stopping...")
                        # Close the websocket immediately to avoid hanging
                        await asyncio.wait_for(websocket.close(), timeout=2.0)
                        break

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except asyncio.TimeoutError:
        logger.info("Websocket close timed out, forcing exit")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        if output:
            output.close()
        logger.info(f"Total events consumed: {event_count}")


async def main():
    config_path = '/config/firehose-consumer.yaml'
    config = Config.from_file(config_path)
    consumer = FirehoseConsumer(config)
    await consumer.start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Bluesky Jetstream Consumer')
    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in local test mode (no Kafka, writes JSON events)'
    )
    parser.add_argument(
        '--max-events',
        type=int,
        default=10,
        help='Maximum number of events to consume in test mode (default: 10)'
    )
    parser.add_argument(
        '--output-file',
        type=str,
        help='Output file for JSON events (optional, defaults to stdout)'
    )
    parser.add_argument(
        '--endpoint',
        type=str,
        help='Jetstream endpoint URL (optional, defaults to wss://jetstream2.us-east.bsky.network/subscribe)'
    )

    args = parser.parse_args()

    if args.test:
        asyncio.run(test_local(args.max_events, args.output_file, args.endpoint))
    else:
        asyncio.run(main())
