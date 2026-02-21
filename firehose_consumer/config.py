import os

FIREHOSE_ENDPOINT = os.environ.get(
    'FIREHOSE_ENDPOINT',
    'wss://jetstream2.us-east.bsky.network/subscribe'
)
FIREHOSE_RECONNECT_DELAY = int(os.environ.get('FIREHOSE_RECONNECT_DELAY', '5'))
FIREHOSE_MAX_RECONNECT_DELAY = int(os.environ.get('FIREHOSE_MAX_RECONNECT_DELAY', '300'))
FIREHOSE_WANTED_COLLECTIONS = os.environ.get(
    'FIREHOSE_WANTED_COLLECTIONS',
    'app.bsky.feed.post,app.bsky.feed.like,app.bsky.feed.repost,app.bsky.graph.follow,app.bsky.graph.block'
)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'redpanda.bluesky.svc.cluster.local:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'bluesky-events')
KAFKA_COMPRESSION_TYPE = os.environ.get('KAFKA_COMPRESSION_TYPE', 'snappy')
KAFKA_LINGER_MS = int(os.environ.get('KAFKA_LINGER_MS', '100'))

HEALTH_PORT = int(os.environ.get('HEALTH_PORT', '8080'))