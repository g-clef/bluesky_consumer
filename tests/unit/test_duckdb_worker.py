import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def s3_env(monkeypatch):
    monkeypatch.setenv('S3_ENDPOINT_URL', 'http://localhost:9000')
    monkeypatch.setenv('S3_ACCESS_KEY_ID', 'test-key')
    monkeypatch.setenv('S3_SECRET_ACCESS_KEY', 'test-secret')
    monkeypatch.setenv('S3_REGION', 'us-east-1')


def test_run_query_returns_rows(tmp_path, monkeypatch):
    # Create a local parquet file for testing (avoids real S3)
    df = pd.DataFrame({
        "did": ["did:plc:abc", "did:plc:xyz"],
        "record_text": ["hello", "world"],
        "collection": ["app.bsky.feed.post", "app.bsky.feed.post"],
    })
    parquet_path = tmp_path / "test.parquet"
    pq.write_table(pa.Table.from_pandas(df), parquet_path)

    # Patch _configure_s3 so it doesn't try to set up S3 (local file needs no httpfs)
    with patch("query_service.duckdb_worker._configure_s3"):
        from query_service.duckdb_worker import _run_query
        result = _run_query([str(parquet_path)], "SELECT did FROM events ORDER BY did")

    assert result == [
        {"did": "did:plc:abc"},
        {"did": "did:plc:xyz"},
    ]


def test_run_query_with_filter(tmp_path, monkeypatch):
    df = pd.DataFrame({
        "did": ["did:plc:abc", "did:plc:xyz"],
        "collection": ["app.bsky.feed.post", "app.bsky.feed.like"],
    })
    parquet_path = tmp_path / "test.parquet"
    pq.write_table(pa.Table.from_pandas(df), parquet_path)

    with patch("query_service.duckdb_worker._configure_s3"):
        from query_service.duckdb_worker import _run_query
        result = _run_query(
            [str(parquet_path)],
            "SELECT did FROM events WHERE collection = 'app.bsky.feed.post'"
        )

    assert result == [{"did": "did:plc:abc"}]


def test_run_query_empty_result(tmp_path):
    df = pd.DataFrame({"did": ["did:plc:abc"], "collection": ["app.bsky.feed.post"]})
    parquet_path = tmp_path / "test.parquet"
    pq.write_table(pa.Table.from_pandas(df), parquet_path)

    with patch("query_service.duckdb_worker._configure_s3"):
        from query_service.duckdb_worker import _run_query
        result = _run_query([str(parquet_path)], "SELECT did FROM events WHERE did = 'nobody'")

    assert result == []


def test_query_file_is_ray_remote():
    from query_service.duckdb_worker import query_file
    assert hasattr(query_file, 'remote'), "query_file must be decorated with @ray.remote"


def test_configure_s3_sets_endpoint(monkeypatch):
    monkeypatch.setenv('S3_ENDPOINT_URL', 'http://minio.example.com:9000')
    monkeypatch.setenv('S3_ACCESS_KEY_ID', 'mykey')
    monkeypatch.setenv('S3_SECRET_ACCESS_KEY', 'mysecret')
    monkeypatch.setenv('S3_REGION', 'us-east-1')

    executed_sql = []
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = lambda sql: executed_sql.append(sql)

    from query_service.duckdb_worker import _configure_s3
    _configure_s3(mock_conn)

    sql_joined = "\n".join(executed_sql)
    assert "minio.example.com:9000" in sql_joined
    assert "mykey" in sql_joined
    assert "path" in sql_joined  # s3_url_style='path' for MinIO compatibility