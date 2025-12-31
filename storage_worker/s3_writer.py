import logging
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import List, Dict, Any, Optional
import os
import socket
from config import S3Config, StorageConfig
from health import events_written, batches_written, s3_write_duration, buffer_size

logger = logging.getLogger(__name__)


def flatten_value(v: Any, sep: str = '__') -> Any:
    if isinstance(v, dict):
        return flatten_dict(v, sep=sep)
    elif isinstance(v, list):
        return [flatten_value(item, sep=sep) for item in v]
    else:
        return v


def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '__') -> Dict[str, Any]:
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, flatten_value(v, sep=sep)))
    return dict(items)


class S3Writer:

    def __init__(self, s3_config: S3Config, storage_config: StorageConfig, local_dir: Optional[str] = None):
        self.s3_config = s3_config
        self.storage_config = storage_config
        self.buffer: List[Dict[str, Any]] = []
        self.worker_id = self._get_worker_id()
        self.local_dir = Path(local_dir) if local_dir else None

        if self.local_dir:
            self.local_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"S3 writer initialized in LOCAL MODE, output: {self.local_dir}, worker: {self.worker_id}")
            self.s3_client = None
        else:
            self.s3_client = boto3.client(
                's3',
                endpoint_url=s3_config.endpoint_url,
                aws_access_key_id=s3_config.access_key_id,
                aws_secret_access_key=s3_config.secret_access_key,
                region_name=s3_config.region
            )

            logger.info(f"S3 writer initialized for bucket: {s3_config.bucket}, worker: {self.worker_id}")

            self._ensure_bucket()

    def _get_worker_id(self) -> str:
        hostname = os.environ.get('HOSTNAME', socket.gethostname())
        return hostname

    def _ensure_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=self.s3_config.bucket)
            logger.info(f"Bucket {self.s3_config.bucket} exists")
        except Exception:
            logger.info(f"Creating bucket {self.s3_config.bucket}")
            try:
                self.s3_client.create_bucket(Bucket=self.s3_config.bucket)
            except Exception as create_error:
                logger.error(f"Failed to create bucket: {create_error}")

    def add_event(self, event: Dict[str, Any]):
        self.buffer.append(event)
        buffer_size.set(len(self.buffer))

    def should_flush(self) -> bool:
        return len(self.buffer) >= self.storage_config.buffer_size

    def flush(self) -> bool:
        if not self.buffer:
            return True

        try:
            with s3_write_duration.time():
                flattened_events = [flatten_dict(event) for event in self.buffer]

                df = pd.DataFrame(flattened_events)

                if 'collection_timestamp' in df.columns:
                    df['collection_timestamp'] = pd.to_datetime(df['collection_timestamp'], unit='ms')
                if 'event_timestamp' in df.columns:
                    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], unit='ms')

                table = pa.Table.from_pandas(df)

                s3_key = self._generate_s3_key()
                event_count = len(self.buffer)

                if self.local_dir:
                    local_path = self.local_dir / s3_key
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    pq.write_table(table, local_path, compression='snappy')
                    logger.info(f"Wrote {event_count} events to {local_path}")
                else:
                    parquet_buffer = BytesIO()
                    pq.write_table(table, parquet_buffer, compression='snappy')
                    parquet_buffer.seek(0)

                    self.s3_client.put_object(
                        Bucket=self.s3_config.bucket,
                        Key=s3_key,
                        Body=parquet_buffer.getvalue()
                    )
                    logger.info(f"Wrote {event_count} events to s3://{self.s3_config.bucket}/{s3_key}")

                events_written.inc(event_count)
                batches_written.inc()

                self.buffer = []
                buffer_size.set(0)

                return True

        except Exception as e:
            logger.error(f"Failed to flush: {e}", exc_info=True)
            return False

    def _generate_s3_key(self) -> str:
        now = datetime.now(timezone.utc)

        partition_path = self.storage_config.partition_format.format(
            year=now.year,
            month=f"{now.month:02d}",
            day=f"{now.day:02d}",
            hour=f"{now.hour:02d}"
        )

        timestamp = int(now.timestamp())
        filename = f"{self.worker_id}_{timestamp}.parquet"

        return f"{partition_path}/{filename}"

    def get_buffer_size(self) -> int:
        return len(self.buffer)
