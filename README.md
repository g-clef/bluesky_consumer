# Bluesky Firehose Consumer

A horizontally scalable data collection system for the Bluesky firehose. Collects and stores raw event metadata from the AT Protocol firehose for later analysis.

## Architecture

```
Bluesky Firehose (WebSocket)
    ↓
Firehose Consumer (1 pod)
    ↓
Redpanda (3-node cluster)
    ↓
Storage Workers (3-10 pods, auto-scaling)
    ↓
MinIO S3 (Parquet files)
```

### Components

1. **Firehose Consumer** - Single pod that connects to the Bluesky WebSocket firehose and produces events to Redpanda
2. **Redpanda** - Kafka-compatible message broker for buffering and distributing events
3. **Storage Workers** - Horizontally scalable workers that consume from Redpanda and write batched Parquet files to S3
4. **MinIO S3** - Object storage for Parquet data files (partitioned by time)

### Why This Architecture?

- **Horizontal Scalability**: Storage workers can scale from 3 to 10 pods based on CPU usage
- **Fault Tolerance**: Redpanda provides buffering and replay capability
- **Data Safety**: Events are persisted to Redpanda before being written to S3
- **High Throughput**: Handles thousands of events per second
- **Cost Effective**: Parquet compression and columnar storage reduce storage costs

## Data Collection

The system collects the following metadata from each event:

- `collection_timestamp` - When we received the event
- `event_timestamp` - When the event occurred
- `did` - Account Decentralized Identifier
- `event_type` - Type of event (commit/identity/account/handle)
- `collection` - Collection type (e.g., app.bsky.feed.post)
- `action` - Action type (create/update/delete)
- `cid` - Content Identifier
- `record_text` - Post text (if applicable)
- `reply_parent` - Parent DID for replies
- `reply_root` - Root DID for reply threads
- `embed_type` - Type of embeds (images/video/external/record)
- `raw_event` - Full JSON event for future processing

## Storage Format

Data is stored in Parquet format with the following partition structure:

```
s3://bluesky-data/
├── year=2025/
│   └── month=12/
│       └── day=28/
│           └── hour=14/
│               ├── storage-worker-0_1735344000.parquet
│               ├── storage-worker-1_1735344060.parquet
│               └── storage-worker-2_1735344120.parquet
```

This partitioning allows:
- Efficient time-range queries
- Easy data management and retention policies
- Partition pruning in query engines (Athena, DuckDB, Spark)

## Deployment

### Prerequisites

- Kubernetes cluster (tested on K3s)
- ArgoCD installed
- MinIO S3 storage (see [homelab-k8s-cluster](https://github.com/g-clef/homelab-k8s-cluster))
- Metrics Server for HPA

### Quick Start

1. **Update S3 Credentials** (if different from defaults):

```bash
# Copy template and edit
cp kubernetes/storage-worker/secret.yaml.template kubernetes/storage-worker/secret.yaml
# Edit with your MinIO credentials
```

2. **Deploy via ArgoCD**:

Add to your app-of-apps repository ([homelab-argo-aoa](https://github.com/g-clef/homelab-argo-aoa)):

```bash
cp argocd/application.yaml /path/to/homelab-argo-aoa/apps/bluesky-consumer.yaml
git add apps/bluesky-consumer.yaml
git commit -m "Add Bluesky consumer application"
git push
```

ArgoCD will automatically:
1. Create the `bluesky` namespace
2. Deploy Redpanda (3 nodes)
3. Deploy Firehose Consumer (1 replica)
4. Deploy Storage Workers (3 replicas, auto-scaling to 10)

3. **Verify Deployment**:

```bash
# Check pods
kubectl get pods -n bluesky

# Check firehose consumer logs
kubectl logs -n bluesky -l app=firehose-consumer -f

# Check storage worker logs
kubectl logs -n bluesky -l app=storage-worker -f

# Check metrics
kubectl get --raw /apis/custom.metrics.k8s.io/v1beta1/namespaces/bluesky/pods/*/bluesky_firehose_events_received_total
```

### Manual Deployment (without ArgoCD)

```bash
# Apply all manifests
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/redpanda/
kubectl apply -f kubernetes/firehose-consumer/
kubectl apply -f kubernetes/storage-worker/

# Wait for Redpanda to be ready
kubectl wait --for=condition=ready pod -l app=redpanda -n bluesky --timeout=300s

# Check status
kubectl get all -n bluesky
```

## Building Docker Images

Images are published to Docker Hub under `gclef/*`:

```bash
# Build and push firehose consumer
docker buildx build --platform linux/amd64,linux/arm64 \
  -f Dockerfile.firehose \
  -t gclef/bluesky-firehose-consumer:latest \
  --push .

# Build and push storage worker
docker buildx build --platform linux/amd64,linux/arm64 \
  -f Dockerfile.worker \
  -t gclef/bluesky-storage-worker:latest \
  --push .
```

## Monitoring

### Metrics

Both services expose Prometheus metrics on port 8080 at `/metrics`:

**Firehose Consumer:**
- `bluesky_firehose_events_received_total` - Events received from firehose
- `bluesky_firehose_events_produced_total` - Events sent to Kafka
- `bluesky_firehose_connection_status` - Connection status (1=connected)
- `bluesky_firehose_reconnections_total` - Reconnection count
- `bluesky_producer_errors_total` - Producer error count

**Storage Worker:**
- `bluesky_storage_events_consumed_total` - Events consumed from Kafka
- `bluesky_storage_events_written_total` - Events written to S3
- `bluesky_storage_batches_written_total` - Batches written to S3
- `bluesky_storage_s3_write_duration_seconds` - S3 write latency
- `bluesky_storage_consumer_lag` - Kafka consumer lag
- `bluesky_storage_buffer_size` - Current buffer size

ServiceMonitor resources are included for Prometheus Operator.

### Health Checks

Both services provide health endpoints:

- `/healthz` - Liveness probe
- `/ready` - Readiness probe
- `/metrics` - Prometheus metrics

## Scaling

### Storage Workers

Workers auto-scale based on CPU and memory utilization:

- **Min replicas**: 3
- **Max replicas**: 10
- **Scale up**: When CPU > 70% or Memory > 80%
- **Scale down**: After 5 minutes of low usage

Manual scaling:

```bash
kubectl scale deployment storage-worker -n bluesky --replicas=5
```

### Redpanda

Redpanda runs 3 nodes by default. To scale:

```bash
kubectl scale statefulset redpanda -n bluesky --replicas=5
```

Note: Ensure topic partition count supports the number of workers (currently 12 partitions).

## Configuration

Configuration is managed via ConfigMaps:

- `firehose-consumer-config` - Firehose and Kafka settings
- `storage-worker-config` - Kafka, S3, and storage settings
- `redpanda-config` - Topic configuration

To update:

```bash
kubectl edit configmap firehose-consumer-config -n bluesky
kubectl rollout restart deployment firehose-consumer -n bluesky
```

## Data Access

Query data using S3-compatible tools:

### DuckDB

```python
import duckdb

conn = duckdb.connect()
conn.execute("""
  SELECT did, event_type, COUNT(*) as count
  FROM read_parquet('s3://bluesky-data/year=2025/month=12/day=28/**/*.parquet')
  WHERE hour = '14'
  GROUP BY did, event_type
  ORDER BY count DESC
  LIMIT 10
""")
```

### AWS CLI (with MinIO)

```bash
aws s3 ls s3://bluesky-data/ \
  --endpoint-url http://minio.minio.svc.cluster.local \
  --recursive
```

## Troubleshooting

### Firehose Consumer Not Connecting

```bash
# Check logs
kubectl logs -n bluesky -l app=firehose-consumer

# Verify network connectivity
kubectl exec -n bluesky -it deployment/firehose-consumer -- \
  curl -v wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos
```

### Storage Workers Not Writing

```bash
# Check S3 connectivity
kubectl exec -n bluesky -it deployment/storage-worker -- \
  python -c "import boto3; s3=boto3.client('s3', endpoint_url='http://minio.minio.svc.cluster.local'); print(s3.list_buckets())"

# Check Kafka lag
kubectl exec -n bluesky -it redpanda-0 -- \
  rpk topic describe bluesky-events
```

### High Consumer Lag

If workers fall behind:

1. Increase worker replicas (up to 10)
2. Increase buffer size or decrease flush interval
3. Check S3 write latency metrics

## Resource Usage

Based on testing:

- **Firehose Consumer**: ~500m CPU, ~1Gi RAM
- **Storage Worker** (each): ~500m CPU, ~1Gi RAM
- **Redpanda** (each node): ~1 CPU, ~2Gi RAM

Total for default deployment: ~6 CPU, ~12Gi RAM

## Related Projects

- [bluesky-experiments](https://github.com/g-clef/bluesky-experiments) - Analysis design and experiments
- [homelab-k8s-cluster](https://github.com/g-clef/homelab-k8s-cluster) - Kubernetes cluster setup
- [homelab-argo-aoa](https://github.com/g-clef/homelab-argo-aoa) - ArgoCD app-of-apps

## License

See [LICENSE](LICENSE) file for details.
