import duckdb
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import MagicMock, patch, call


@pytest.fixture(autouse=True)
def env_vars(monkeypatch):
    monkeypatch.setenv('S3_ENDPOINT_URL', 'http://localhost:9000')
    monkeypatch.setenv('S3_BUCKET', 'bluesky-data')
    monkeypatch.setenv('S3_REGION', 'us-east-1')
    monkeypatch.setenv('S3_ACCESS_KEY_ID', 'test-key')
    monkeypatch.setenv('S3_SECRET_ACCESS_KEY', 'test-secret')
    monkeypatch.setenv('STORAGE_PARTITION_FORMAT', 'year={year}/month={month}/day={day}/hour={hour}')
    monkeypatch.setenv('DEFAULT_QUERY_LIMIT', '10000')
    monkeypatch.setenv('RAY_ADDRESS', 'ray://localhost:10001')


# --- _apply_limit ---

def test_apply_limit_below_limit():
    from query_service.serve_app import _apply_limit
    rows = [{"a": 1}, {"a": 2}]
    result, truncated = _apply_limit(rows, limit=10)
    assert result == rows
    assert truncated is False


def test_apply_limit_exactly_at_limit():
    from query_service.serve_app import _apply_limit
    rows = [{"a": i} for i in range(5)]
    result, truncated = _apply_limit(rows, limit=5)
    assert len(result) == 5
    assert truncated is False


def test_apply_limit_above_limit():
    from query_service.serve_app import _apply_limit
    rows = [{"a": i} for i in range(10)]
    result, truncated = _apply_limit(rows, limit=3)
    assert result == [{"a": 0}, {"a": 1}, {"a": 2}]
    assert truncated is True


def test_apply_limit_none_means_no_limit():
    from query_service.serve_app import _apply_limit
    rows = [{"a": i} for i in range(1000)]
    result, truncated = _apply_limit(rows, limit=None)
    assert len(result) == 1000
    assert truncated is False


# --- _reduce_rows ---

def test_reduce_rows_simple_select():
    from query_service.serve_app import _reduce_rows
    rows = [{"did": "abc", "collection": "post"}, {"did": "xyz", "collection": "like"}]
    result = _reduce_rows(rows, "SELECT did FROM events ORDER BY did")
    assert result == [{"did": "abc"}, {"did": "xyz"}]


def test_reduce_rows_group_by_aggregation():
    from query_service.serve_app import _reduce_rows
    # Simulate partial results from two workers with overlapping groups
    partial_rows = [
        {"collection": "app.bsky.feed.post", "cnt": 700},
        {"collection": "app.bsky.feed.like", "cnt": 200},
        {"collection": "app.bsky.feed.post", "cnt": 300},
    ]
    result = _reduce_rows(
        partial_rows,
        "SELECT collection, SUM(cnt) as cnt FROM events GROUP BY collection ORDER BY cnt DESC"
    )
    assert result == [
        {"collection": "app.bsky.feed.post", "cnt": 1000},
        {"collection": "app.bsky.feed.like", "cnt": 200},
    ]


def test_reduce_rows_empty_input():
    from query_service.serve_app import _reduce_rows
    result = _reduce_rows([], "SELECT did FROM events")
    assert result == []


# --- _write_to_s3 ---

def test_write_to_s3_puts_object(tmp_path):
    from query_service.serve_app import _write_to_s3
    rows = [{"did": "abc", "record_text": "hello"}]
    mock_s3 = MagicMock()

    with patch("query_service.serve_app._make_s3_client", return_value=mock_s3):
        count = _write_to_s3(rows, "s3://my-bucket/results/out.parquet")

    assert count == 1
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "my-bucket"
    assert call_kwargs["Key"] == "results/out.parquet"


# --- _do_list ---

def test_do_list_returns_paths():
    from query_service.serve_app import _do_list
    with patch("query_service.serve_app._make_s3_client") as mock_client_fn, \
         patch("query_service.serve_app.list_parquet_files") as mock_list:
        mock_list.return_value = ["s3://bluesky-data/year=2026/month=04/day=26/hour=10/f.parquet"]
        result = _do_list("2026-04-26T10:00:00Z", "2026-04-26T10:59:59Z")

    assert result == {"paths": ["s3://bluesky-data/year=2026/month=04/day=26/hour=10/f.parquet"]}
    assert mock_list.called


# --- _do_query ---

def test_do_query_inline_with_limit():
    from query_service.serve_app import _do_query
    mock_rows = [{"did": f"did:plc:{i}"} for i in range(5)]
    with patch("query_service.serve_app._run_query", return_value=mock_rows):
        result = _do_query(["s3://bucket/f.parquet"], "SELECT did FROM events", limit=3, output_path=None)

    assert result["row_count"] == 3
    assert result["truncated"] is True
    assert len(result["rows"]) == 3


def test_do_query_inline_no_truncation():
    from query_service.serve_app import _do_query
    mock_rows = [{"did": "did:plc:abc"}]
    with patch("query_service.serve_app._run_query", return_value=mock_rows):
        result = _do_query(["s3://bucket/f.parquet"], "SELECT did FROM events", limit=10, output_path=None)

    assert result["truncated"] is False
    assert result["row_count"] == 1


def test_do_query_output_path():
    from query_service.serve_app import _do_query
    mock_rows = [{"did": "did:plc:abc"}]
    with patch("query_service.serve_app._run_query", return_value=mock_rows), \
         patch("query_service.serve_app._write_to_s3", return_value=1) as mock_write:
        result = _do_query(
            ["s3://bucket/f.parquet"], "SELECT did FROM events",
            limit=10, output_path="s3://out-bucket/result.parquet"
        )

    assert "output_path" in result
    assert result["output_path"] == "s3://out-bucket/result.parquet"
    assert "rows" not in result
    mock_write.assert_called_once_with(mock_rows, "s3://out-bucket/result.parquet")


# --- _do_distribute ---

def test_do_distribute_collects_and_reduces():
    from query_service.serve_app import _do_distribute
    with patch("query_service.serve_app._make_s3_client"), \
         patch("query_service.serve_app.list_parquet_files", return_value=[
             "s3://b/file1.parquet", "s3://b/file2.parquet"
         ]), \
         patch("query_service.serve_app.query_file") as mock_worker, \
         patch("query_service.serve_app.ray") as mock_ray:

        mock_future_1 = MagicMock()
        mock_future_2 = MagicMock()
        mock_worker.remote.side_effect = [mock_future_1, mock_future_2]
        mock_ray.get.side_effect = [
            [{"collection": "post", "cnt": 700}],
            [{"collection": "post", "cnt": 300}],
        ]

        result = _do_distribute(
            "2026-04-26T10:00:00Z", "2026-04-26T10:59:59Z",
            "SELECT collection, SUM(cnt) as cnt FROM events GROUP BY collection",
            limit=100, output_path=None
        )

    assert result["failed_files"] == 0
    assert result["row_count"] == 1
    assert result["rows"][0]["cnt"] == 1000


def test_do_distribute_handles_worker_failure():
    from query_service.serve_app import _do_distribute
    with patch("query_service.serve_app._make_s3_client"), \
         patch("query_service.serve_app.list_parquet_files", return_value=[
             "s3://b/file1.parquet", "s3://b/file2.parquet"
         ]), \
         patch("query_service.serve_app.query_file") as mock_worker, \
         patch("query_service.serve_app.ray") as mock_ray:

        mock_worker.remote.side_effect = [MagicMock(), MagicMock()]
        mock_ray.get.side_effect = [
            [{"did": "abc"}],
            RuntimeError("worker crashed"),
        ]

        result = _do_distribute(
            "2026-04-26T10:00:00Z", "2026-04-26T10:59:59Z",
            "SELECT did FROM events",
            limit=100, output_path=None
        )

    assert result["failed_files"] == 1
    assert result["row_count"] == 1  # only file1's row survived


def test_do_distribute_no_files_found():
    from query_service.serve_app import _do_distribute
    with patch("query_service.serve_app._make_s3_client"), \
         patch("query_service.serve_app.list_parquet_files", return_value=[]):
        result = _do_distribute(
            "2026-04-26T10:00:00Z", "2026-04-26T10:59:59Z",
            "SELECT did FROM events",
            limit=100, output_path=None
        )

    assert result == {"rows": [], "row_count": 0, "truncated": False, "failed_files": 0}


# --- REST endpoint tests ---

from starlette.testclient import TestClient


def _get_client():
    # Import here to avoid Ray Serve decorator running at collection time
    from query_service.serve_app import _starlette
    return TestClient(_starlette)


def test_list_endpoint_success():
    with patch("query_service.serve_app._do_list", return_value={"paths": ["s3://b/f.parquet"]}):
        client = _get_client()
        resp = client.post("/list", json={"start_time": "2026-04-26T10:00:00Z", "end_time": "2026-04-26T10:59:59Z"})
    assert resp.status_code == 200
    assert resp.json() == {"paths": ["s3://b/f.parquet"]}


def test_list_endpoint_error_returns_500():
    with patch("query_service.serve_app._do_list", side_effect=RuntimeError("S3 down")):
        client = _get_client()
        resp = client.post("/list", json={"start_time": "2026-04-26T10:00:00Z", "end_time": "2026-04-26T10:59:59Z"})
    assert resp.status_code == 500
    assert "S3 down" in resp.json()["error"]


def test_query_endpoint_success():
    mock_result = {"rows": [{"did": "abc"}], "row_count": 1, "truncated": False}
    with patch("query_service.serve_app._do_query", return_value=mock_result):
        client = _get_client()
        resp = client.post("/query", json={
            "paths": ["s3://b/f.parquet"],
            "sql": "SELECT did FROM events",
            "limit": 100,
        })
    assert resp.status_code == 200
    assert resp.json() == mock_result


def test_query_endpoint_bad_sql_returns_400():
    with patch("query_service.serve_app._do_query", side_effect=duckdb.Error("syntax error")):
        client = _get_client()
        resp = client.post("/query", json={
            "paths": ["s3://b/f.parquet"],
            "sql": "INVALID SQL !!!",
        })
    assert resp.status_code == 400
    assert "syntax error" in resp.json()["error"]


def test_distribute_endpoint_success():
    mock_result = {"rows": [], "row_count": 0, "truncated": False, "failed_files": 0}
    with patch("query_service.serve_app._do_distribute", return_value=mock_result):
        client = _get_client()
        resp = client.post("/distribute", json={
            "start_time": "2026-04-26T10:00:00Z",
            "end_time": "2026-04-26T10:59:59Z",
            "sql": "SELECT did FROM events",
        })
    assert resp.status_code == 200
    assert resp.json() == mock_result