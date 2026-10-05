# Quick Start Guide

Get the E2E Data Pipeline running locally in under 5 minutes.

## Prerequisites

- **Docker Desktop** (v24+) with at least **8GB RAM** allocated (16GB recommended for full stack)
- **Docker Compose** v2+ (included with Docker Desktop)
- **Make** (optional, for convenience commands)
- **Python 3.10+** (optional, for running tests locally)

> **Low-resource machines (8GB RAM)?** Use the lite profile: `docker compose -f docker-compose.yaml -f docker-compose.lite.yaml up -d` -- disables Elasticsearch, InfluxDB, MLflow and reduces memory limits.

## 1. Clone & Configure

```bash
git clone https://github.com/hoangsonww/End-to-End-Data-Pipeline.git
cd End-to-End-Data-Pipeline

# Create environment file from template
cp .env.example .env
```

## 2. Build & Start

```bash
# Build all images and start services
make build && make up

# Or without Make:
docker compose build
docker compose up -d
```

This starts **20 services** (~18GB RAM at peak). Initial startup takes 2-3 minutes while services initialize.

For smaller machines, use the lite profile (disables 4 non-essential services, ~8GB RAM):

```bash
make up-lite
# Or: docker compose -f docker-compose.yaml -f docker-compose.lite.yaml up -d
```

## 3. Verify Health

```bash
# Check all services are healthy
make health

# Or:
docker compose ps
```

Wait until all services show `(healthy)` status. The Airflow services take the longest (~60s).

## 4. Access Service UIs

```bash
make urls
```

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / airflow_admin_2024 |
| **Grafana** | http://localhost:3000 | admin / admin_secret_2024 |
| **MinIO Console** | http://localhost:9001 | minio / minio_secret_2024 |
| **MLflow** | http://localhost:5001 | - |
| **Spark Master** | http://localhost:8081 | - |
| **API Swagger** | http://localhost:5000/swagger | - |
| **Prometheus** | http://localhost:9090 | - |
| **Elasticsearch** | http://localhost:9200 | - |

## 5. Run the Pipeline

### Trigger Batch ETL

```bash
make trigger-batch
# Or: docker compose exec airflow-webserver airflow dags trigger batch_ingestion_dag
```

This runs: MySQL → Validation → MinIO → Spark Transform → PostgreSQL

### Trigger Warehouse Transform

```bash
make trigger-warehouse
# Or: docker compose exec airflow-webserver airflow dags trigger warehouse_transform_dag
```

This loads data into the star schema (dimensions → facts → aggregations).

### View Kafka Topics

```bash
make kafka-topics
```

The `kafka-producer` service automatically generates sensor data to the `sensor_readings` topic.

### Run Spark Jobs Manually

```bash
make spark-batch    # Batch ETL
make spark-stream   # Streaming (runs continuously)
```

## 6. Run Tests

```bash
# Install test dependencies
pip install -r requirements.txt

# Run all 35 tests
make test
```

## 7. Stop & Cleanup

```bash
# Stop all services (preserves data)
make down

# Stop and delete all data volumes
make clean
```

## Common Operations

| Command | Description |
|---------|-------------|
| `make up` | Start all services |
| `make down` | Stop all services |
| `make build` | Build Docker images |
| `make rebuild` | Full rebuild from scratch |
| `make logs` | Tail all service logs |
| `make logs-kafka` | Tail Kafka logs only |
| `make health` | Check service health |
| `make status` | Show service status |
| `make shell-postgres` | Open psql shell |
| `make list-dags` | List Airflow DAGs |
| `make validate` | Validate compose config |

## Troubleshooting

**Services won't start:** Ensure Docker has at least 8GB RAM. Check `make logs` for errors.

**Airflow unhealthy:** Airflow takes ~60s to initialize. Run `make logs-airflow-webserver` to check progress.

**Port conflicts:** Check if ports 3000, 5000, 8080, 9000, 9090, 9092 are available.

**Spark job fails:** Ensure MinIO has the `raw-data` bucket. Run `make logs-minio-init` to verify bucket creation.

## Snowflake Integration (Optional)

The pipeline supports **Snowflake** as the primary data warehouse. Without it, PostgreSQL serves as fallback.

```bash
# Set credentials in .env
SNOWFLAKE_ACCOUNT=your_account.us-east-1
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password

# Initialize the Snowflake schema (run once)
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -f snowflake/init_warehouse.sql

# The warehouse_transform_dag auto-detects Snowflake and stages data accordingly
make trigger-warehouse
```

## Deployment Options

All deployment methods use a single universal script (`scripts/deploy.sh`):

### Local / On-Premise

```bash
# Full stack (20 services, ~18GB RAM)
make deploy-local

# Lite mode (16 services, ~8GB RAM)
make deploy-lite

# Any VM (DigitalOcean droplet, bare metal, etc.)
ssh your-server
git clone https://github.com/hoangsonww/End-to-End-Data-Pipeline.git
cd End-to-End-Data-Pipeline && cp .env.example .env
make deploy-local
```

### Kubernetes (Any Provider)

```bash
# Generic Kubernetes (any cluster with kubectl configured)
make deploy-k8s

# Or with provider-specific optimizations:
make deploy-aws      # AWS EKS (Terraform + Helm, gp3 storage, ALB ingress)
make deploy-gcp      # GCP GKE (Helm, pd-ssd storage)
make deploy-azure    # Azure AKS (Helm, managed-premium storage)
make deploy-onprem   # On-prem k3s/kubeadm (Helm, reduced resources)
```

### Helm Chart (Manual)

```bash
# Install directly with Helm
helm repo add bitnami https://charts.bitnami.com/bitnami && helm repo update

helm install e2e-pipeline ./helm/e2e-pipeline \
  -f helm/e2e-pipeline/values-aws.yaml \
  --set postgresql.auth.password=YOUR_PASSWORD \
  --set minio.auth.rootPassword=YOUR_PASSWORD \
  --namespace pipeline --create-namespace
```

### AWS Infrastructure (Terraform)

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars  # Edit with your settings
terraform init && terraform plan && terraform apply
# Then: make deploy-aws
```

Provisions: VPC (public/private subnets), EKS cluster with autoscaling, RDS PostgreSQL (encrypted, backups), S3 data lake (versioned, encrypted), IAM roles, security groups.

### Provider Comparison

| Target | Command | Requirements | Min Resources | Storage Class |
|--------|---------|-------------|---------------|---------------|
| **Docker (full)** | `make deploy-local` | Docker | 16GB, 14 CPU | Docker volumes |
| **Docker (lite)** | `make deploy-lite` | Docker | 8GB, 7 CPU | Docker volumes |
| **AWS EKS** | `make deploy-aws` | Terraform, AWS CLI, Helm | 2x t3.large | gp3 |
| **GCP GKE** | `make deploy-gcp` | gcloud, Helm | 2x e2-standard-4 | pd-ssd |
| **Azure AKS** | `make deploy-azure` | az CLI, Helm | 2x Standard_D4s_v3 | managed-premium |
| **On-prem** | `make deploy-onprem` | kubectl, Helm | 16GB, 8 CPU | local-path |
| **Any K8s** | `make deploy-k8s` | kubectl, Helm | Depends | Default SC |

### Teardown

```bash
make deploy-teardown    # Removes Docker Compose or Helm deployment
make deploy-status      # Check what's currently running
```

See [DEPLOYMENT_STRATEGIES.md](DEPLOYMENT_STRATEGIES.md) for Blue/Green and Canary rollout strategies.

---

For architecture details, see [ARCHITECTURE.md](ARCHITECTURE.md).
