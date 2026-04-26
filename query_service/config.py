import os

S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', 'http://minio.minio.svc.cluster.local')
S3_BUCKET = os.environ.get('S3_BUCKET', 'bluesky-data')
S3_REGION = os.environ.get('S3_REGION', 'us-east-1')
S3_ACCESS_KEY_ID = os.environ.get('S3_ACCESS_KEY_ID', '')
S3_SECRET_ACCESS_KEY = os.environ.get('S3_SECRET_ACCESS_KEY', '')
STORAGE_PARTITION_FORMAT = os.environ.get(
    'STORAGE_PARTITION_FORMAT',
    'year={year}/month={month}/day={day}/hour={hour}'
)
DEFAULT_QUERY_LIMIT = int(os.environ.get('DEFAULT_QUERY_LIMIT', '10000'))
RAY_ADDRESS = os.environ.get(
    'RAY_ADDRESS',
    'ray://homelab-ray-head-svc.ray.svc.cluster.local:10001'
)