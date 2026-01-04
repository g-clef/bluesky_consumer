# Sealed Secrets Setup Guide

This guide explains how to manage the S3 credentials using Sealed Secrets. it assumes that 
the sealed secret operator has already been installed in the cluster.

## Prerequisites

**Install kubeseal CLI** on your local machine:
   ```bash
   # macOS
   brew install kubeseal

   # Linux
   wget https://github.com/bitnami-labs/sealed-secrets/releases/download/v0.24.0/kubeseal-0.24.0-linux-amd64.tar.gz
   tar xfz kubeseal-0.24.0-linux-amd64.tar.gz
   sudo install -m 755 kubeseal /usr/local/bin/kubeseal
   ```

## Generating Encrypted Secrets

### Step 1: Fetch the Public Key

First, retrieve the public key from your cluster's Sealed Secrets controller:

```bash
kubeseal --fetch-cert --controller-name=sealed-secrets-controller --controller-namespace=kube-system > pub-cert.pem
```

**Important:** Add `pub-cert.pem` to your `.gitignore` - this file should NOT be committed to git.

### Step 2: Create and Encrypt Your Secret

Replace `YOUR_ACCESS_KEY` and `YOUR_SECRET_KEY` with your actual MinIO/S3 credentials:

```bash
kubectl create secret generic s3-credentials \
  --namespace=bluesky \
  --from-literal=access-key-id='YOUR_ACCESS_KEY' \
  --from-literal=secret-access-key='YOUR_SECRET_KEY' \
  --dry-run=client -o yaml | \
  kubeseal --cert=pub-cert.pem --format=yaml > kubernetes/storage-worker/sealed-secret.yaml
```

This will overwrite the template with the actual encrypted values.

### Step 3: Commit and Push

```bash
git add kubernetes/storage-worker/sealed-secret.yaml
git commit -m "Update S3 credentials sealed secret"
git push
```

ArgoCD will automatically sync and the Sealed Secrets controller will decrypt it in-cluster.

## Alternative: Update Specific Fields Only

If you want to update just one field and preserve the existing structure:

```bash
# Encrypt just the access-key-id
echo -n 'YOUR_ACCESS_KEY' | kubeseal --raw --cert=pub-cert.pem \
  --scope=namespace-wide \
  --namespace=bluesky \
  --name=s3-credentials

# Encrypt just the secret-access-key
echo -n 'YOUR_SECRET_KEY' | kubeseal --raw --cert=pub-cert.pem \
  --scope=namespace-wide \
  --namespace=bluesky \
  --name=s3-credentials
```

Then manually replace the encrypted values in `kubernetes/storage-worker/sealed-secret.yaml`.

## Rotating Secrets

To rotate credentials:

1. Update your MinIO/S3 credentials
2. Re-run Step 2 above with the new credentials
3. Commit and push
4. The storage-worker pods will automatically get the new credentials on next restart, or you can force a rollout:
   ```bash
   kubectl rollout restart deployment/storage-worker -n bluesky
   ```

## Verifying the Secret

After ArgoCD syncs, verify the secret was created:

```bash
kubectl get secret s3-credentials -n bluesky
kubectl describe secret s3-credentials -n bluesky
```

You can also verify the decrypted values (be careful with this in production):
```bash
kubectl get secret s3-credentials -n bluesky -o jsonpath='{.data.access-key-id}' | base64 -d
```

## Troubleshooting

### Secret not being created
- Check if the Sealed Secrets controller is running:
  ```bash
  kubectl get pods -n kube-system -l name=sealed-secrets-controller
  ```
- Check the controller logs:
  ```bash
  kubectl logs -n kube-system -l name=sealed-secrets-controller
  ```

### ArgoCD showing out of sync
- The SealedSecret object itself should be in sync
- The decrypted Secret is managed by the controller, not ArgoCD
- Add this to your ArgoCD Application if needed:
  ```yaml
  spec:
    ignoreDifferences:
    - group: ""
      kind: Secret
      name: s3-credentials
      jsonPointers:
      - /data
  ```

## Security Notes

1. **Never commit the plain Secret** - Always use `--dry-run=client` and pipe to kubeseal
2. **Protect the private key** - The Sealed Secrets controller's private key is critical. Back it up securely.
3. **Public key is safe to share** - The public cert can be committed to git if desired
4. **Rotate regularly** - Change your S3 credentials periodically
5. **Audit access** - Monitor who can access the Sealed Secrets controller namespace

## Backup and Disaster Recovery

Backup the Sealed Secrets controller's private key:

```bash
kubectl get secret -n kube-system sealed-secrets-key -o yaml > sealed-secrets-key-backup.yaml
```

Store this backup in a secure location (NOT in git). You'll need it to restore the controller if the cluster is rebuilt.