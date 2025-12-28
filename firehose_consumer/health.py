"""Health check HTTP server for Kubernetes probes."""
import logging
from aiohttp import web
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

logger = logging.getLogger(__name__)

# Metrics
events_received = Counter('bluesky_firehose_events_received_total', 'Total events received from firehose')
events_produced = Counter('bluesky_firehose_events_produced_total', 'Total events produced to Kafka')
connection_status = Gauge('bluesky_firehose_connection_status', 'Connection status (1=connected, 0=disconnected)')
reconnections = Counter('bluesky_firehose_reconnections_total', 'Total number of reconnections')
producer_errors = Counter('bluesky_producer_errors_total', 'Total producer errors')


class HealthServer:
    """HTTP server for health checks and metrics."""

    def __init__(self, port: int):
        self.port = port
        self.ready = False
        self.app = web.Application()
        self.app.router.add_get('/healthz', self.liveness)
        self.app.router.add_get('/ready', self.readiness)
        self.app.router.add_get('/metrics', self.metrics)
        self.runner = None

    async def liveness(self, request):
        """Liveness probe - is the process alive?"""
        return web.Response(text='ok')

    async def readiness(self, request):
        """Readiness probe - is it connected to firehose?"""
        if self.ready:
            return web.Response(text='ready')
        return web.Response(text='not ready', status=503)

    async def metrics(self, request):
        """Prometheus metrics endpoint."""
        metrics_output = generate_latest()
        return web.Response(body=metrics_output, content_type=CONTENT_TYPE_LATEST)

    async def start(self):
        """Start the health check server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '0.0.0.0', self.port)
        await site.start()
        logger.info(f"Health check server started on port {self.port}")

    async def stop(self):
        """Stop the health check server."""
        if self.runner:
            await self.runner.cleanup()

    def set_ready(self, ready: bool):
        """Set readiness status."""
        self.ready = ready