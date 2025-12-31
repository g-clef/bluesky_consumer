"""Health check HTTP server for Kubernetes probes."""
import logging
from aiohttp import web
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

logger = logging.getLogger(__name__)

events_consumed = Counter('bluesky_storage_events_consumed_total', 'Total events consumed from Kafka')
events_written = Counter('bluesky_storage_events_written_total', 'Total events written to S3')
batches_written = Counter('bluesky_storage_batches_written_total', 'Total batches written to S3')
s3_write_duration = Histogram('bluesky_storage_s3_write_duration_seconds', 'S3 write duration')
consumer_lag = Gauge('bluesky_storage_consumer_lag', 'Consumer lag')
buffer_size = Gauge('bluesky_storage_buffer_size', 'Current buffer size')


class HealthServer:

    def __init__(self, port: int):
        self.port = port
        self.ready = False
        self.app = web.Application()
        self.app.router.add_get('/healthz', self.liveness)
        self.app.router.add_get('/ready', self.readiness)
        self.app.router.add_get('/metrics', self.metrics)
        self.runner = None

    async def liveness(self, request):
        return web.Response(text='ok')

    async def readiness(self, request):
        if self.ready:
            return web.Response(text='ready')
        return web.Response(text='not ready', status=503)

    async def metrics(self, request):
        metrics_output = generate_latest()
        return web.Response(body=metrics_output, content_type=CONTENT_TYPE_LATEST)

    async def start(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '0.0.0.0', self.port)
        await site.start()
        logger.info(f"Health check server started on port {self.port}")

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

    def set_ready(self, ready: bool):
        self.ready = ready
