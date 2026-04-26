import asyncio
import logging
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import boto3
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import ray

from query_service import config
from query_service.duckdb_worker import _run_query, query_file
from query_service.s3_lister import list_parquet_files

logger = logging.getLogger(__name__)


def _make_s3_client():
    return boto3.client(
        's3',
        endpoint_url=config.S3_ENDPOINT_URL,
        aws_access_key_id=config.S3_ACCESS_KEY_ID,
        aws_secret_access_key=config.S3_SECRET_ACCESS_KEY,
        region_name=config.S3_REGION,
    )


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace('Z', '+00:00'))


def _apply_limit(rows: List[Dict], limit: Optional[int]):
    if limit is not None and len(rows) > limit:
        return rows[:limit], True
    return rows, False


def _reduce_rows(partial_rows: List[Dict], sql: str) -> List[Dict]:
    if not partial_rows:
        return []
    df = pd.DataFrame(partial_rows)
    conn = duckdb.connect()
    conn.register("events", df)
    rel = conn.execute(sql)
    columns = [desc[0] for desc in rel.description]
    rows = [dict(zip(columns, row)) for row in rel.fetchall()]
    conn.close()
    return rows


def _write_to_s3(rows: List[Dict], output_path: str) -> int:
    _, _, rest = output_path.partition("s3://")
    bucket, _, key = rest.partition("/")
    table = pa.Table.from_pandas(pd.DataFrame(rows))
    buf = BytesIO()
    pq.write_table(table, buf, compression='snappy')
    buf.seek(0)
    _make_s3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    return len(rows)


def _do_list(start_time: str, end_time: str) -> Dict:
    start = _parse_iso(start_time)
    end = _parse_iso(end_time)
    paths = list_parquet_files(start, end, config.S3_BUCKET, config.STORAGE_PARTITION_FORMAT, _make_s3_client())
    return {"paths": paths}


def _do_query(paths: List[str], sql: str, limit: Optional[int], output_path: Optional[str]) -> Dict:
    rows = _run_query(paths, sql)
    if output_path:
        return {"output_path": output_path, "row_count": _write_to_s3(rows, output_path)}
    rows, truncated = _apply_limit(rows, limit)
    return {"rows": rows, "row_count": len(rows), "truncated": truncated}


def _do_distribute(
    start_time: str, end_time: str, sql: str,
    limit: Optional[int], output_path: Optional[str],
) -> Dict:
    start = _parse_iso(start_time)
    end = _parse_iso(end_time)
    paths = list_parquet_files(start, end, config.S3_BUCKET, config.STORAGE_PARTITION_FORMAT, _make_s3_client())

    if not paths:
        return {"rows": [], "row_count": 0, "truncated": False, "failed_files": 0}

    futures = [query_file.remote(path, sql) for path in paths]
    partial_rows: List[Dict] = []
    failed_files = 0
    for future in futures:
        try:
            partial_rows.extend(ray.get(future))
        except Exception as e:
            logger.error("Worker failed: %s", e)
            failed_files += 1

    final_rows = _reduce_rows(partial_rows, sql)

    if output_path:
        return {
            "output_path": output_path,
            "row_count": _write_to_s3(final_rows, output_path),
            "failed_files": failed_files,
        }

    final_rows, truncated = _apply_limit(final_rows, limit)
    return {"rows": final_rows, "row_count": len(final_rows), "truncated": truncated, "failed_files": failed_files}


from fastmcp import FastMCP
from ray import serve
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route


# --- MCP tools ---

mcp = FastMCP("bluesky-query")


@mcp.tool()
def list_parquet_files(start_time: str, end_time: str) -> List[str]:
    """List S3 paths for all parquet files in the given ISO 8601 time range. Data is queryable as table 'events'."""
    return _do_list(start_time, end_time)["paths"]


@mcp.tool()
def query_partition(
    paths: List[str],
    sql: str,
    limit: int = 10000,
    output_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Run SQL against a specific list of parquet S3 paths. Reference the data as table 'events'."""
    return _do_query(paths, sql, None if output_path else limit, output_path)


@mcp.tool()
def distribute_query(
    start_time: str,
    end_time: str,
    sql: str,
    limit: int = 10000,
    output_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Find parquet files for a time range and distribute SQL across Ray workers. Reference data as table 'events'."""
    return _do_distribute(start_time, end_time, sql, None if output_path else limit, output_path)


# --- Starlette handlers ---

async def list_handler(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        result = await asyncio.to_thread(_do_list, body["start_time"], body["end_time"])
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def query_handler(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        limit = body.get("limit", config.DEFAULT_QUERY_LIMIT)
        output_path = body.get("output_path")
        result = await asyncio.to_thread(
            _do_query, body["paths"], body["sql"],
            None if output_path else limit, output_path,
        )
        return JSONResponse(result)
    except duckdb.Error as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


async def distribute_handler(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        limit = body.get("limit", config.DEFAULT_QUERY_LIMIT)
        output_path = body.get("output_path")
        result = await asyncio.to_thread(
            _do_distribute, body["start_time"], body["end_time"], body["sql"],
            None if output_path else limit, output_path,
        )
        return JSONResponse(result)
    except duckdb.Error as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# --- Starlette app (module-level so TestClient can import it) ---

_starlette = Starlette(routes=[
    Route("/list", list_handler, methods=["POST"]),
    Route("/query", query_handler, methods=["POST"]),
    Route("/distribute", distribute_handler, methods=["POST"]),
    Mount("/mcp", app=mcp.http_app()),
])


# --- Ray Serve deployment ---
# Pass a builder function to @serve.ingress to avoid pickling the Starlette app
# at import time (cloudpickle cannot serialize weakref.ReferenceType in Starlette).

def _build_starlette_app():
    return _starlette


@serve.deployment(num_replicas=1)
@serve.ingress(_build_starlette_app)
class BlueskyQueryService:
    pass


app = BlueskyQueryService.bind()


# --- Entrypoint (driver pod) ---

if __name__ == "__main__":
    ray.init(
        address=config.RAY_ADDRESS,
        runtime_env={
            "pip": ["duckdb", "boto3", "pyarrow", "pandas", "fastmcp"],
            "working_dir": "/app",
            "env_vars": {k: v for k, v in {
                "S3_ENDPOINT_URL": config.S3_ENDPOINT_URL,
                "S3_BUCKET": config.S3_BUCKET,
                "S3_REGION": config.S3_REGION,
                "S3_ACCESS_KEY_ID": config.S3_ACCESS_KEY_ID,
                "S3_SECRET_ACCESS_KEY": config.S3_SECRET_ACCESS_KEY,
                "STORAGE_PARTITION_FORMAT": config.STORAGE_PARTITION_FORMAT,
                "DEFAULT_QUERY_LIMIT": str(config.DEFAULT_QUERY_LIMIT),
            }.items() if v},
        },
    )
    serve.run(app, blocking=True)