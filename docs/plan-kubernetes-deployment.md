# Kubernetes Deployment Plan: stocker-store

## Prerequisites
- Ensure the `stocker` namespace exists: `kubectl create namespace stocker`
- Ensure the registry secret exists: `kubectl create secret docker-registry wheeli-registry --docker-server=git.wheeli.ca --docker-username=<user> --docker-password=<pass> -n stocker`

## Deployment Steps
1. Create a ConfigMap for non-secret env vars (GRPC_PORT, S3_REGION, STOCK_TTL, etc.).
2. Create a Secret for DATABASE_URL, S3_ACCESS_KEY, S3_SECRET_KEY, and any other credentials.
3. Apply the Kubernetes Deployment manifest with TCP probe on port 3500.
4. Apply the Kubernetes Service manifest (ClusterIP `stocker-store` on port 3500).

## Verification Steps
- Verify pods are running: `kubectl get pods -n stocker -l app=${service.name}`
- Check pod logs for errors: `kubectl logs -n stocker -l app=${service.name}`
- Verify the gRPC Service is ready: `kubectl get svc stocker-store -n stocker`
- Verify other services can resolve `stocker-store.stocker.svc:3500`.
