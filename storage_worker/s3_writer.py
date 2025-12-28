"""S3 writer with Parquet batching."""
import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from io import BytesIO
from typing import List, Dict, Any
import os
import socket
from .config import S3Config, StorageConfig
from .health import events_written, batches_written, s3_write_duration, buffer_size

logger = logging.getLogger(__name__)


class S3Writer:
    """Batch events and write Parquet files to S3."""

    def __init__(self, s3_config: S3Config, storage_config: StorageConfig):
        self.s3_config = s3_config
        self.storage_config = storage_config
        self.buffer: List[Dict[str, Any]] = []
        self.worker_id = self._get_worker_id()

        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            endpoint_url=s3_config.endpoint_url,
            aws_access_key_id=s3_config.access_key_id,
            aws_secret_access_key=s3_config.secret_access_key,
            region_name=s3_config.region
        )

        logger.info(f"S3 writer initialized for bucket: {s3_config.bucket}, worker: {self.worker_id}")

        # Ensure bucket exists
        self._ensure_bucket()

    def _get_worker_id(self) -> str:
        """Get worker ID from hostname or environment."""
        # In Kubernetes, hostname is the pod name
        hostname = os.environ.get('HOSTNAME', socket.gethostname())
        return hostname

    def _ensure_bucket(self):
        """Ensure S3 bucket exists."""
        try:
            self.s3_client.head_bucket(Bucket=self.s3_config.bucket)
            logger.info(f"Bucket {self.s3_config.bucket} exists")
        except Exception as e:
            logger.info(f"Creating bucket {self.s3_config.bucket}")
            try:
                self.s3_client.create_bucket(Bucket=self.s3_config.bucket)
            except Exception as create_error:
                logger.error(f"Failed to create bucket: {create_error}")
                # Continue anyway - bucket might exist but head_bucket failed

    def add_event(self, event: Dict[str, Any]):
        """Add an event to the buffer."""
        self.buffer.append(event)
        buffer_size.set(len(self.buffer))

    def should_flush(self) -> bool:
        """Check if buffer should be flushed."""
        return len(self.buffer) >= self.storage_config.buffer_size

    def flush(self) -> bool:
        """Flush buffer to S3."""
        if not self.buffer:
            return True

        try:
            with s3_write_duration.time():
                # Convert events to DataFrame
                df = pd.DataFrame(self.buffer)

                # Convert timestamp columns to proper datetime
                if 'collection_timestamp' in df.columns:
                    df['collection_timestamp'] = pd.to_datetime(df['collection_timestamp'], unit='ms')
                if 'event_timestamp' in df.columns:
                    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], unit='ms')

                # Convert to PyArrow table
                table = pa.Table.from_pandas(df)

                # Write to Parquet in memory
                parquet_buffer = BytesIO()
                pq.write_table(table, parquet_buffer, compression='snappy')
                parquet_buffer.seek(0)

                # Generate S3 key with partitioning
                s3_key = self._generate_s3_key()

                # Upload to S3
                self.s3_client.put_object(
                    Bucket=self.s3_config.bucket,
                    Key=s3_key,
                    Body=parquet_buffer.getvalue()
                )

                event_count = len(self.buffer)
                logger.info(f"Wrote {event_count} events to s3://{self.s3_config.bucket}/{s3_key}")

                # Update metrics
                events_written.inc(event_count)
                batches_written.inc()

                # Clear buffer
                self.buffer = []
                buffer_size.set(0)

                return True

        except Exception as e:
            logger.error(f"Failed to flush to S3: {e}", exc_info=True)
            return False

    def _generate_s3_key(self) -> str:
        """Generate S3 key with partitioning."""
        now = datetime.utcnow()

        # Format partition path
        partition_path = self.storage_config.partition_format.format(
            year=now.year,
            month=f"{now.month:02d}",
            day=f"{now.day:02d}",
            hour=f"{now.hour:02d}"
        )

        # Generate unique filename
        timestamp = int(now.timestamp())
        filename = f"{self.worker_id}_{timestamp}.parquet"

        return f"{partition_path}/{filename}"

    def get_buffer_size(self) -> int:
        """Get current buffer size."""
        return len(self.buffer)