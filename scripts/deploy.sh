#!/usr/bin/env bash
# =============================================================
# Universal Deploy Script for E2E Data Pipeline
# =============================================================
# Supports: Docker Compose (local/on-prem), Kubernetes (any provider)
#
# Usage:
#   ./scripts/deploy.sh local          # Docker Compose (full stack)
#   ./scripts/deploy.sh local-lite     # Docker Compose (lite, 8GB RAM)
#   ./scripts/deploy.sh k8s            # Kubernetes via Helm (auto-detect)
#   ./scripts/deploy.sh aws            # AWS EKS via Terraform + Helm
#   ./scripts/deploy.sh gcp            # GCP GKE via Terraform + Helm
#   ./scripts/deploy.sh azure          # Azure AKS via Terraform + Helm
#   ./scripts/deploy.sh status         # Show deployment status
#   ./scripts/deploy.sh teardown       # Remove deployment

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
HELM_CHART="$PROJECT_DIR/helm/e2e-pipeline"
RELEASE_NAME="${PIPELINE_RELEASE_NAME:-e2e-pipeline}"
NAMESPACE="${PIPELINE_NAMESPACE:-pipeline}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[DEPLOY]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# ----- Prerequisite checks -----
check_docker() {
  command -v docker >/dev/null 2>&1 || { err "Docker not found. Install from https://docs.docker.com/get-docker/"; exit 1; }
  docker info >/dev/null 2>&1 || { err "Docker daemon not running."; exit 1; }
  log "Docker: OK"
}

check_kubectl() {
  command -v kubectl >/dev/null 2>&1 || { err "kubectl not found. Install from https://kubernetes.io/docs/tasks/tools/"; exit 1; }
  kubectl cluster-info >/dev/null 2>&1 || { err "Cannot connect to Kubernetes cluster. Check kubeconfig."; exit 1; }
  log "kubectl: OK ($(kubectl config current-context))"
}

check_helm() {
  command -v helm >/dev/null 2>&1 || { err "Helm not found. Install from https://helm.sh/docs/intro/install/"; exit 1; }
  log "Helm: $(helm version --short)"
}

check_terraform() {
  command -v terraform >/dev/null 2>&1 || { err "Terraform not found. Install from https://developer.hashicorp.com/terraform/install"; exit 1; }
  log "Terraform: $(terraform version -json 2>/dev/null | grep -o '"[0-9.]*"' | head -1 || terraform version | head -1)"
}

# ----- Docker Compose deployment -----
deploy_local() {
  log "Deploying with Docker Compose (full stack)..."
  check_docker
  cd "$PROJECT_DIR"

  if [ ! -f .env ]; then
    log "Creating .env from template..."
    cp .env.example .env
  fi

  docker compose build
  docker compose up -d
  log "Waiting for services to initialize..."
  sleep 10
  docker compose ps
  echo ""
  log "Pipeline deployed! Run 'make urls' to see service URLs."
}

deploy_local_lite() {
  log "Deploying with Docker Compose (lite mode, ~8GB RAM)..."
  check_docker
  cd "$PROJECT_DIR"

  if [ ! -f .env ]; then
    cp .env.example .env
  fi

  docker compose -f docker-compose.yaml -f docker-compose.lite.yaml build
  docker compose -f docker-compose.yaml -f docker-compose.lite.yaml up -d
  log "Lite deployment complete. Core services only."
}

# ----- Kubernetes deployment -----
deploy_k8s() {
  log "Deploying to Kubernetes cluster..."
  check_kubectl
  check_helm

  local VALUES_FILE="${1:-}"
  local HELM_ARGS=""

  if [ -n "$VALUES_FILE" ] && [ -f "$VALUES_FILE" ]; then
    HELM_ARGS="-f $VALUES_FILE"
    log "Using values file: $VALUES_FILE"
  fi

  # Add Helm repos
  log "Adding Helm repositories..."
  helm repo add bitnami https://charts.bitnami.com/bitnami 2>/dev/null || true
  helm repo add grafana https://grafana.github.io/helm-charts 2>/dev/null || true
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
  helm repo add elastic https://helm.elastic.co 2>/dev/null || true
  helm repo update

  # Install/upgrade
  log "Installing/upgrading Helm release: $RELEASE_NAME..."
  helm upgrade --install "$RELEASE_NAME" "$HELM_CHART" \
    --namespace "$NAMESPACE" \
    --create-namespace \
    --timeout 10m \
    --wait \
    $HELM_ARGS

  log "Deployment complete!"
  kubectl get pods -n "$NAMESPACE"
}

# ----- Cloud-specific deployments -----
deploy_aws() {
  log "Deploying to AWS (EKS + Terraform)..."
  check_terraform
  check_kubectl
  check_helm

  cd "$PROJECT_DIR/terraform"

  if [ ! -f terraform.tfvars ]; then
    warn "No terraform.tfvars found. Copying example..."
    cp terraform.tfvars.example terraform.tfvars
    err "Edit terraform/terraform.tfvars with your AWS settings, then re-run."
    exit 1
  fi

  log "Running Terraform..."
  terraform init
  terraform plan -out=tfplan
  read -rp "Apply Terraform plan? [y/N] " confirm
  if [[ "$confirm" =~ ^[yY]$ ]]; then
    terraform apply tfplan
    log "Infrastructure provisioned."

    # Update kubeconfig
    CLUSTER_NAME=$(terraform output -raw eks_cluster_name 2>/dev/null || echo "e2e-pipeline")
    REGION=$(grep 'aws_region' terraform.tfvars | awk -F'"' '{print $2}' || echo "us-east-1")
    aws eks update-kubeconfig --name "$CLUSTER_NAME" --region "$REGION"

    # Deploy via Helm
    cd "$PROJECT_DIR"
    deploy_k8s "helm/e2e-pipeline/values-aws.yaml"
  else
    log "Cancelled."
  fi
}

deploy_gcp() {
  log "Deploying to GCP (GKE)..."
  check_kubectl
  check_helm

  command -v gcloud >/dev/null 2>&1 || { err "gcloud CLI not found. Install from https://cloud.google.com/sdk/docs/install"; exit 1; }

  warn "GCP deployment uses Helm chart directly on an existing GKE cluster."
  warn "Ensure you have a GKE cluster and gcloud configured."

  local PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
  log "GCP Project: $PROJECT_ID"

  cd "$PROJECT_DIR"
  deploy_k8s "helm/e2e-pipeline/values-gcp.yaml"
}

deploy_azure() {
  log "Deploying to Azure (AKS)..."
  check_kubectl
  check_helm

  command -v az >/dev/null 2>&1 || { err "Azure CLI not found. Install from https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"; exit 1; }

  warn "Azure deployment uses Helm chart directly on an existing AKS cluster."
  warn "Ensure you have an AKS cluster and az configured."

  cd "$PROJECT_DIR"
  deploy_k8s "helm/e2e-pipeline/values-azure.yaml"
}

# ----- Status & teardown -----
show_status() {
  log "Checking deployment status..."
  if docker compose ps 2>/dev/null | grep -q "Up\|running"; then
    log "Docker Compose deployment found:"
    docker compose ps
  fi
  if kubectl get namespace "$NAMESPACE" 2>/dev/null; then
    log "Kubernetes deployment found in namespace: $NAMESPACE"
    kubectl get pods,svc -n "$NAMESPACE"
  fi
}

teardown() {
  warn "This will remove the deployment."
  read -rp "Continue? [y/N] " confirm
  [[ "$confirm" =~ ^[yY]$ ]] || { log "Cancelled."; exit 0; }

  if docker compose ps 2>/dev/null | grep -q "Up\|running"; then
    log "Stopping Docker Compose..."
    cd "$PROJECT_DIR"
    docker compose down -v
  fi
  if kubectl get namespace "$NAMESPACE" 2>/dev/null; then
    log "Removing Helm release..."
    helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" 2>/dev/null || true
    kubectl delete namespace "$NAMESPACE" --timeout=60s 2>/dev/null || true
  fi
  log "Teardown complete."
}

# ----- Main -----
case "${1:-help}" in
  local)       deploy_local ;;
  local-lite)  deploy_local_lite ;;
  k8s)         deploy_k8s "${2:-}" ;;
  aws)         deploy_aws ;;
  gcp)         deploy_gcp ;;
  azure)       deploy_azure ;;
  status)      show_status ;;
  teardown)    teardown ;;
  *)
    echo "E2E Data Pipeline - Universal Deploy Script"
    echo ""
    echo "Usage: $0 <target>"
    echo ""
    echo "Targets:"
    echo "  local         Docker Compose (full stack, ~18GB RAM)"
    echo "  local-lite    Docker Compose (lite mode, ~8GB RAM)"
    echo "  k8s [values]  Kubernetes via Helm (any cluster)"
    echo "  aws           AWS EKS via Terraform + Helm"
    echo "  gcp           GCP GKE via Helm"
    echo "  azure         Azure AKS via Helm"
    echo "  status        Show deployment status"
    echo "  teardown      Remove deployment"
    ;;
esac
