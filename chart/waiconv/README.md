# waiconv Helm Chart

This Helm chart deploys the waiconv Counter application with Redis on Kubernetes.

## Prerequisites

- Kubernetes cluster
- Helm 3.x
- Docker image built and available (or pushed to a registry)

## Installation

### Build the Docker image (if not already done)

```bash
cd ../app
docker build -t waiconv:latest .
```

### Install the chart

```bash
# From the helm directory
helm install waiconv . --create-namespace --namespace waiconv

# Or with custom values
helm install waiconv . -f values.yaml --namespace waiconv
```

### Access the application

```bash
# Port-forward to access locally
kubectl port-forward -n waiconv service/waiconv 8000:8000

# Then open http://localhost:8000
```

## Configuration

Key configuration values in `values.yaml`:

- `replicaCount`: Number of application replicas
- `image.repository`: Docker image repository
- `image.tag`: Docker image tag
- `config.redis.*`: Redis connection settings
- `config.kafka.*`: Kafka/Confluent Cloud connection settings
- `ingress.enabled`: Enable ingress for external access

## Upgrade

```bash
helm upgrade waiconv . --namespace waiconv
```

## Uninstall

```bash
helm uninstall waiconv --namespace waiconv
```

## Notes

- Redis runs as a single pod (not HA by default)
- Kafka credentials are stored in values.yaml (consider using Secrets for production)
- The application connects to Confluent Cloud for Kafka
