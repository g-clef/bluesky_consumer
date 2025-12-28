"""Configuration management for firehose consumer."""
import os
import yaml
from dataclasses import dataclass
from typing import Optional


@dataclass
class FirehoseConfig:
    """Firehose connection configuration."""
    endpoint: str
    reconnect_delay: int
    max_reconnect_delay: int


@dataclass
class KafkaConfig:
    """Kafka producer configuration."""
    bootstrap_servers: str
    topic: str
    compression_type: str
    batch_size: int
    linger_ms: int


@dataclass
class HealthConfig:
    """Health check server configuration."""
    port: int


@dataclass
class Config:
    """Main configuration."""
    firehose: FirehoseConfig
    kafka: KafkaConfig
    health: HealthConfig

    @classmethod
    def from_file(cls, config_path: Optional[str] = None) -> 'Config':
        """Load configuration from file or environment."""
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                data = yaml.safe_load(f)
        else:
            # Default configuration
            data = {
                'firehose': {
                    'endpoint': 'wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos',
                    'reconnect_delay': 5,
                    'max_reconnect_delay': 300,
                },
                'kafka': {
                    'bootstrap_servers': 'redpanda.bluesky.svc.cluster.local:9092',
                    'topic': 'bluesky-events',
                    'compression_type': 'snappy',
                    'batch_size': 1000,
                    'linger_ms': 100,
                },
                'health': {
                    'port': 8080,
                }
            }

        # Override with environment variables if present
        if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
            data['kafka']['bootstrap_servers'] = os.environ['KAFKA_BOOTSTRAP_SERVERS']
        if 'KAFKA_TOPIC' in os.environ:
            data['kafka']['topic'] = os.environ['KAFKA_TOPIC']

        return cls(
            firehose=FirehoseConfig(**data['firehose']),
            kafka=KafkaConfig(**data['kafka']),
            health=HealthConfig(**data['health'])
        )