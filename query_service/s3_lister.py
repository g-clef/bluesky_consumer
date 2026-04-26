from datetime import datetime, timedelta
from typing import List


def _hourly_prefixes(start: datetime, end: datetime, partition_format: str) -> List[str]:
    prefixes = []
    current = start.replace(minute=0, second=0, microsecond=0)
    while current <= end:
        prefixes.append(partition_format.format(
            year=current.year,
            month=f"{current.month:02d}",
            day=f"{current.day:02d}",
            hour=f"{current.hour:02d}",
        ))
        current += timedelta(hours=1)
    return prefixes


def list_parquet_files(
    start_time: datetime,
    end_time: datetime,
    bucket: str,
    partition_format: str,
    s3_client,
) -> List[str]:
    paths = []
    for prefix in _hourly_prefixes(start_time, end_time, partition_format):
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet'):
                    paths.append(f"s3://{bucket}/{obj['Key']}")
    return paths