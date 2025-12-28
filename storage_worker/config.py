"""Configuration management for storage worker."""
import os
import yaml
from dataclasses import dataclass
from typing import Optional


@dataclass
class KafkaConfig:
    """Kafka consumer configuration."""
    bootstrap_servers: str
    topic: str
    group_id: str
    auto_offset_reset: str
    enable_auto_commit: bool


@dataclass
class S3Config:
    """S3 storage configuration."""
    endpoint_url: str
    bucket: str
    region: str
    access_key_id: str
    secret_access_key: str


@dataclass
class StorageConfig:
    """Storage behavior configuration."""
    buffer_size: int
    flush_interval_seconds: int
    partition_format: str


@dataclass
class HealthConfig:
    """Health check server configuration."""
    port: int


@dataclass
class Config:
    """Main configuration."""
    kafka: KafkaConfig
    s3: S3Config
    storage: StorageConfig
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
                'kafka': {
                    'bootstrap_servers': 'redpanda.bluesky.svc.cluster.local:9092',
                    'topic': 'bluesky-events',
                    'group_id': 'bluesky-storage-workers',
                    'auto_offset_reset': 'earliest',
                    'enable_auto_commit': False,
                },
                's3': {
                    'endpoint_url': 'http://minio.minio.svc.cluster.local',
                    'bucket': 'bluesky-data',
                    'region': 'us-east-1',
                    'access_key_id': '',
                    'secret_access_key': '',
                },
                'storage': {
                    'buffer_size': 10000,
                    'flush_interval_seconds': 60,
                    'partition_format': 'year={year}/month={month}/day={day}/hour={hour}',
                },
                'health': {
                    'port': 8080,
                }
            }

        # Override with environment variables
        if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
            data['kafka']['bootstrap_servers'] = os.environ['KAFKA_BOOTSTRAP_SERVERS']
        if 'S3_ENDPOINT_URL' in os.environ:
            data['s3']['endpoint_url'] = os.environ['S3_ENDPOINT_URL']
        if 'S3_BUCKET' in os.environ:
            data['s3']['bucket'] = os.environ['S3_BUCKET']
        if 'S3_ACCESS_KEY_ID' in os.environ:
            data['s3']['access_key_id'] = os.environ['S3_ACCESS_KEY_ID']
        if 'S3_SECRET_ACCESS_KEY' in os.environ:
            data['s3']['secret_access_key'] = os.environ['S3_SECRET_ACCESS_KEY']

        return cls(
            kafka=KafkaConfig(**data['kafka']),
            s3=S3Config(**data['s3']),
            storage=StorageConfig(**data['storage']),
            health=HealthConfig(**data['health'])
        )