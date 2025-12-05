#!/bin/bash

# Install Flink Operator and Base Resources
# This script can be run independently if cluster already exists

set -e

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_info() {
    echo -e "${YELLOW}➜${NC} $1"
}

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Installing Base Resources${NC}"
echo -e "${BLUE}========================================${NC}"

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Apply base resources
echo ""
print_info "Applying namespace and service account..."
kubectl apply -f "$PROJECT_ROOT/k8s/base/namespace.yaml"
kubectl apply -f "$PROJECT_ROOT/k8s/base/serviceaccount.yaml"
print_status "Namespace and RBAC configured"

# Apply PVs and PVCs
print_info "Creating persistent volumes..."
kubectl apply -f "$PROJECT_ROOT/k8s/base/pv-savepoints.yaml"
kubectl apply -f "$PROJECT_ROOT/k8s/base/pv-checkpoints.yaml"
print_status "Persistent volumes created"

print_info "Creating persistent volume claims..."
kubectl apply -f "$PROJECT_ROOT/k8s/base/pvc-savepoints.yaml"
kubectl apply -f "$PROJECT_ROOT/k8s/base/pvc-checkpoints.yaml"
print_status "Persistent volume claims created"

# Wait for PVCs to be bound
print_info "Waiting for PVCs to be bound..."
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/flink-savepoints-pvc -n flink --timeout=60s
kubectl wait --for=jsonpath='{.status.phase}'=Bound pvc/flink-checkpoints-pvc -n flink --timeout=60s
print_status "PVCs are bound"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Base Resources Installed!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Resources created:"
kubectl get pv | grep flink
echo ""
kubectl get pvc -n flink
echo ""
print_status "Base resources are ready"
