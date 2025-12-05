#!/bin/bash

# Teardown Flink Test Cluster
# Cleanly removes all Flink deployments and deletes kind cluster

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
CLUSTER_NAME=${CLUSTER_NAME:-flink-test}

print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_info() {
    echo -e "${YELLOW}➜${NC} $1"
}

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Flink Test Cluster Teardown${NC}"
echo -e "${BLUE}========================================${NC}"

# Check if kind cluster exists
if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    print_info "Kind cluster '$CLUSTER_NAME' does not exist"
    exit 0
fi

# Try to clean up Flink resources gracefully
print_info "Attempting to clean up Flink resources..."

# Switch to the cluster context
kubectl config use-context "kind-${CLUSTER_NAME}" 2>/dev/null || true

# Delete all Flink deployments
print_info "Deleting all Flink deployments..."
kubectl delete flinkdeployment --all -n flink --wait=false 2>/dev/null || true
print_status "Flink deployments deleted"

# Wait a bit for graceful shutdown
sleep 3

# Delete Kafka resources if they exist
print_info "Deleting Kafka resources (if any)..."
kubectl delete kafka --all -n flink --wait=false 2>/dev/null || true
kubectl delete kafkatopic --all -n flink --wait=false 2>/dev/null || true

# Uninstall Flink operator
print_info "Uninstalling Flink Kubernetes Operator..."
helm uninstall flink-kubernetes-operator -n flink --wait --timeout=60s 2>/dev/null || true
print_status "Flink operator uninstalled"

# Delete kind cluster
echo ""
print_info "Deleting kind cluster: $CLUSTER_NAME..."
kind delete cluster --name "$CLUSTER_NAME"

print_status "Kind cluster deleted"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Teardown Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "To setup a new cluster, run:"
echo "  ./scripts/setup-cluster.sh"
echo ""
