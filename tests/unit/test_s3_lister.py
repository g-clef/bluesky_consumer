from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from query_service.s3_lister import _hourly_prefixes, list_parquet_files

PARTITION_FORMAT = "year={year}/month={month}/day={day}/hour={hour}"


def test_hourly_prefixes_single_hour():
    start = datetime(2026, 4, 26, 10, 30, tzinfo=timezone.utc)
    end = datetime(2026, 4, 26, 10, 59, tzinfo=timezone.utc)
    assert _hourly_prefixes(start, end, PARTITION_FORMAT) == [
        "year=2026/month=04/day=26/hour=10"
    ]


def test_hourly_prefixes_day_boundary():
    start = datetime(2026, 4, 25, 23, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 26, 1, 0, tzinfo=timezone.utc)
    assert _hourly_prefixes(start, end, PARTITION_FORMAT) == [
        "year=2026/month=04/day=25/hour=23",
        "year=2026/month=04/day=26/hour=00",
        "year=2026/month=04/day=26/hour=01",
    ]


def test_hourly_prefixes_month_boundary():
    start = datetime(2026, 3, 31, 23, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
    assert _hourly_prefixes(start, end, PARTITION_FORMAT) == [
        "year=2026/month=03/day=31/hour=23",
        "year=2026/month=04/day=01/hour=00",
    ]


def test_list_parquet_files_returns_s3_paths():
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {"Contents": [
            {"Key": "year=2026/month=04/day=26/hour=10/worker_1.parquet"},
            {"Key": "year=2026/month=04/day=26/hour=10/worker_2.parquet"},
        ]}
    ]

    start = datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 26, 10, 59, tzinfo=timezone.utc)
    result = list_parquet_files(start, end, "bluesky-data", PARTITION_FORMAT, mock_s3)

    assert result == [
        "s3://bluesky-data/year=2026/month=04/day=26/hour=10/worker_1.parquet",
        "s3://bluesky-data/year=2026/month=04/day=26/hour=10/worker_2.parquet",
    ]


def test_list_parquet_files_empty_bucket():
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{}]  # no 'Contents' key

    start = datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 26, 10, 59, tzinfo=timezone.utc)
    result = list_parquet_files(start, end, "bluesky-data", PARTITION_FORMAT, mock_s3)
    assert result == []


def test_list_parquet_files_skips_non_parquet():
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {"Contents": [
            {"Key": "year=2026/month=04/day=26/hour=10/worker_1.parquet"},
            {"Key": "year=2026/month=04/day=26/hour=10/_metadata"},
        ]}
    ]

    start = datetime(2026, 4, 26, 10, 0, tzinfo=timezone.utc)
    end = datetime(2026, 4, 26, 10, 59, tzinfo=timezone.utc)
    result = list_parquet_files(start, end, "bluesky-data", PARTITION_FORMAT, mock_s3)
    assert result == [
        "s3://bluesky-data/year=2026/month=04/day=26/hour=10/worker_1.parquet"
    ]