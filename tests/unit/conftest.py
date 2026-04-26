"""
Pytest configuration for storage_worker tests.

Sets up sys.path and mocks external infrastructure dependencies
(confluent_kafka, boto3) before any storage_worker modules are imported.
"""
import os
import sys
from unittest.mock import MagicMock

# Add project root to path so imports like `import storage_worker.storage_worker as sw`
# and `from query_service.s3_lister import ...` resolve correctly.
_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Also add storage_worker/ to path so bare imports (import config, from consumer import ...)
# in storage_worker modules resolve correctly when running tests.
# This must be added AFTER the project root so both import styles work.
_sw_dir = os.path.join(_project_root, 'storage_worker')
if _sw_dir not in sys.path:
    sys.path.insert(1, _sw_dir)

# Mock confluent_kafka before consumer.py is imported.
# KafkaException must be a real exception class so except-clauses work.
_confluent_mock = MagicMock()
_confluent_mock.KafkaException = Exception
_confluent_mock.KafkaError = MagicMock()
sys.modules['confluent_kafka'] = _confluent_mock

# Mock boto3 before s3_writer.py is imported.
sys.modules['boto3'] = MagicMock()

# Provide minimal environment variables required by config.py.
_defaults = {
    'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
    'KAFKA_TOPIC': 'test-topic',
    'KAFKA_GROUP_ID': 'test-group',
    'S3_ENDPOINT_URL': 'http://localhost:9000',
    'S3_BUCKET': 'test-bucket',
    'S3_REGION': 'us-east-1',
    'S3_ACCESS_KEY_ID': 'test-key',
    'S3_SECRET_ACCESS_KEY': 'test-secret',
    'STORAGE_BUFFER_SIZE': '100',
    'STORAGE_FLUSH_INTERVAL_SECONDS': '60',
}
for key, value in _defaults.items():
    os.environ.setdefault(key, value)