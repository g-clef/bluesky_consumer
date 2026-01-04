# ArgoCD App-of-Apps Configuration

This directory contains the ArgoCD Application manifests for deploying the Bluesky Consumer stack using the App-of-Apps pattern.

## Structure

```
argocd/
├── application.yaml          # Root app-of-apps (deploys all apps below)
└── apps/
    ├── namespace.yaml        # Namespace application
    ├── redpanda.yaml         # Redpanda messaging application
    ├── firehose-consumer.yaml # Firehose consumer application
    └── storage-worker.yaml   # Storage worker application
```

## How It Works

1. **Root Application** (`application.yaml`):
   - Deployed manually or via ArgoCD bootstrap
   - Points to `argocd/apps/` directory
   - Creates all child applications

2. **Child Applications** (in `apps/`):
   - Each component has its own Application manifest
   - Uses Kustomize for resource management
   - Can be synced independently
   - Sync waves ensure proper deployment order:
     - Wave `-1`: Namespace
     - Wave `0`: Redpanda (infrastructure)
     - Wave `1`: Firehose Consumer & Storage Worker (applications)

## Deployment Order

```
1. bluesky-namespace      (wave -1)  Creates the namespace
   └─> bluesky-redpanda   (wave 0)   Deploys message broker
       ├─> bluesky-firehose-consumer (wave 1)  Deploys consumer
       └─> bluesky-storage-worker    (wave 1)  Deploys worker
```

## Initial Setup

### Option 1: Deploy via ArgoCD UI

1. Login to ArgoCD UI
2. Click "New App"
3. Use these settings:
   - **Application Name**: `bluesky-consumer`
   - **Project**: `default`
   - **Sync Policy**: `Automatic`
   - **Repository URL**: `https://github.com/g-clef/bluesky_consumer`
   - **Revision**: `main`
   - **Path**: `argocd/apps`
   - **Cluster**: `https://kubernetes.default.svc`
   - **Namespace**: `argocd`

### Option 2: Deploy via kubectl

```bash
kubectl apply -f argocd/application.yaml
```

This will create the root app, which will automatically create all child applications.

## Managing Applications

### Sync All Applications

The root app-of-apps automatically syncs all child apps. To manually sync:

```bash
argocd app sync bluesky-consumer
```

### Sync Individual Component

```bash
# Sync just the storage worker
argocd app sync bluesky-storage-worker

# Sync just redpanda
argocd app sync bluesky-redpanda
```

### View All Applications

```bash
argocd app list | grep bluesky
```

### Check Status

```bash
# All apps
argocd app get bluesky-consumer --show-operation

# Specific app
argocd app get bluesky-storage-worker
```

### Disable Auto-Sync for Debugging

If you need to manually control sync for debugging:

```bash
argocd app set bluesky-storage-worker --sync-policy none
```

Re-enable:
```bash
argocd app set bluesky-storage-worker --sync-policy automated
```

## Benefits of This Structure

1. **Independent Syncing**: Each component can be synced/deployed independently
2. **Clear Dependencies**: Sync waves ensure proper deployment order
3. **Better Organization**: Each component is self-contained
4. **Easier Debugging**: Issues isolated to specific components
5. **Selective Rollback**: Roll back individual components without affecting others
6. **Kustomize Integration**: Each component uses Kustomize for resource management

## Troubleshooting

### Application Not Syncing

Check the application status:
```bash
argocd app get bluesky-<component-name>
```

Check sync status:
```bash
argocd app sync bluesky-<component-name> --dry-run
```

### Resource Conflicts

If you see conflicts with existing resources:
```bash
# Delete the old deployment
kubectl delete -n bluesky deployment <name>

# Sync the application
argocd app sync bluesky-<component-name>
```

### View Sync Waves

```bash
argocd app manifests bluesky-consumer | grep sync-wave
```

## Modifications

### Adding a New Component

1. Create Kustomize resources in `kubernetes/<component-name>/`
2. Create an Application manifest in `argocd/apps/<component-name>.yaml`
3. Set appropriate sync-wave annotation
4. Commit and push - ArgoCD will auto-sync

### Changing Sync Order

Modify the `argocd.argoproj.io/sync-wave` annotation in the Application manifest.
Lower numbers deploy first (can be negative).

## Clean Up

To remove everything:

```bash
# Delete the root app (cascades to all child apps)
argocd app delete bluesky-consumer --cascade

# Or via kubectl
kubectl delete -f argocd/application.yaml
```

This will remove all child applications and their resources (due to finalizers).