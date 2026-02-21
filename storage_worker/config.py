import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'redpanda.bluesky.svc.cluster.local:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'bluesky-events')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'bluesky-storage-workers')
KAFKA_AUTO_OFFSET_RESET = os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest')
KAFKA_ENABLE_AUTO_COMMIT = os.environ.get('KAFKA_ENABLE_AUTO_COMMIT', 'false').lower() == 'true'

S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', 'http://minio.minio.svc.cluster.local')
S3_BUCKET = os.environ.get('S3_BUCKET', 'bluesky-data')
S3_REGION = os.environ.get('S3_REGION', 'us-east-1')
S3_ACCESS_KEY_ID = os.environ.get('S3_ACCESS_KEY_ID', '')
S3_SECRET_ACCESS_KEY = os.environ.get('S3_SECRET_ACCESS_KEY', '')

STORAGE_BUFFER_SIZE = int(os.environ.get('STORAGE_BUFFER_SIZE', '10000'))
STORAGE_FLUSH_INTERVAL_SECONDS = int(os.environ.get('STORAGE_FLUSH_INTERVAL_SECONDS', '60'))
STORAGE_PARTITION_FORMAT = os.environ.get(
    'STORAGE_PARTITION_FORMAT',
    'year={year}/month={month}/day={day}/hour={hour}'
)

HEALTH_PORT = int(os.environ.get('HEALTH_PORT', '8080'))