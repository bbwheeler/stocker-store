# Kubernetes Deployment Design: stocker-store

## Overview
Kubernetes deployment strategy for the `stocker-store` gRPC service. This is the central store for stock data and scores; other services depend on its DNS name to connect via gRPC.

## Architecture
- **Namespace**: `stocker`
- **Registry Authentication**: Uses `wheeli-registry` image pull secret.
- **Deployment Strategy**: Rolling Update.
- **Service**: ClusterIP Service named `stocker-store` on port 3500 (gRPC). This is the DNS name that other services depend on: `stocker-store.stocker.svc:3500`.

## Resource Requirements
- **CPU Limit**: 500m
- **Memory Limit**: 512Mi

## Configuration (Environment Variables)
The following environment variables are used by the service:
    - DATABASE_URL: <required, no default> — Postgres DSN (postgres://… or postgresql://…) or host:port/db form
    - DATABASE_USERNAME: <optional; only used when DATABASE_URL lacks a user portion>
    - DATABASE_PASSWORD: <optional; only used when DATABASE_URL lacks a password portion>
    - GRPC_PORT: 3500 (default; gRPC listen port)
    - RABBITMQ_URL: **required** (no placeholder value shown; see below). The code's built-in default uses a real shared-broker password but this should be explicitly set in-cluster to ensure the `stocker` user. An empty `RABBITMQ_URL` disables ingestion entirely. Must be stored as Secret, not ConfigMap.
    - RABBITMQ_EXCHANGE: stock.update (default)
    - RABBITMQ_EXCHANGE_TYPE: topic (default)
    - RABBITMQ_ROUTING_KEY: stock.update.v1 (default)
    - RABBITMQ_QUEUE: stocker-store.stock.update.v1 (default)
    - RABBITMQ_PREFETCH: 1 (default)
    - RABBITMQ_OUTPUT_EXCHANGE: <optional; when set, enables new-stock publisher>
    - RABBITMQ_OUTPUT_ROUTING_KEY: stock.created.v1 (default)
    - S3_ENDPOINT: <required; no default in code>
    - S3_REGION: us-east-1 (default)
    - S3_BUCKET: <required; no default>
    - STOCK_TTL: 720h (default, Go duration string; set to "0" to disable expiry)

## Connectivity & Probes
- **Liveness/Readiness Probes**: tcpSocket on port 3500 (gRPC port — the service has no HTTP health endpoint)
- **Service Exposure**: ClusterIP gRPC Service named `stocker-store` on port 3500
  - **Critical dependency**: gem, investor, and summary-evaluator all connect to stocker-store via its DNS name `stocker-store.stocker.svc:3500`. Do not use localhost or the pod IP in client configs.

## Dependencies
- PostgreSQL: <configured via DATABASE_URL>
- RabbitMQ: `192.168.1.48:5672` (consumes from stock.update exchange)
