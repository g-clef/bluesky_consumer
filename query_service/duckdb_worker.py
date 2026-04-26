import os
from typing import Any, Dict, List

import duckdb
import ray


def _configure_s3(conn: duckdb.DuckDBPyConnection) -> None:
    endpoint = os.environ['S3_ENDPOINT_URL'].replace('https://', '').replace('http://', '')
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{endpoint}';")
    conn.execute(f"SET s3_access_key_id='{os.environ['S3_ACCESS_KEY_ID']}';")
    conn.execute(f"SET s3_secret_access_key='{os.environ['S3_SECRET_ACCESS_KEY']}';")
    conn.execute(f"SET s3_region='{os.environ.get('S3_REGION', 'us-east-1')}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")


def _run_query(paths: List[str], sql: str) -> List[Dict[str, Any]]:
    conn = duckdb.connect()
    _configure_s3(conn)
    paths_literal = ", ".join(f"'{p}'" for p in paths)
    conn.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet([{paths_literal}])")
    rel = conn.execute(sql)
    columns = [desc[0] for desc in rel.description]
    rows = [dict(zip(columns, row)) for row in rel.fetchall()]
    conn.execute("DROP VIEW IF EXISTS events")
    conn.close()
    return rows


@ray.remote
def query_file(path: str, sql: str) -> List[Dict[str, Any]]:
    return _run_query([path], sql)