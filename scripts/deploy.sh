#!/bin/bash

# Deploy Flink Job to Kind Cluster
# Builds, loads image, and deploys to Kubernetes

set -e

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Configuration
CLUSTER_NAME=${CLUSTER_NAME:-flink-test}
IMAGE_NAME=${IMAGE_NAME:-flink-deployment-test:latest}
NAMESPACE=${NAMESPACE:-flink}
DEPLOYMENT_FILE=${1:-k8s/test/stateful-test-pvc.yaml}

print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_info() {
    echo -e "${YELLOW}➜${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Flink Job Deployment${NC}"
echo -e "${BLUE}========================================${NC}"

# Verify kind cluster exists
if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    print_error "Kind cluster '$CLUSTER_NAME' not found!"
    echo ""
    echo "Please run setup first:"
    echo "  ./scripts/setup-cluster.sh"
    exit 1
fi
print_status "Kind cluster '$CLUSTER_NAME' found"

# Verify we're in the right kubectl context
CURRENT_CONTEXT=$(kubectl config current-context)
EXPECTED_CONTEXT="kind-${CLUSTER_NAME}"
if [ "$CURRENT_CONTEXT" != "$EXPECTED_CONTEXT" ]; then
    print_info "Switching kubectl context to $EXPECTED_CONTEXT"
    kubectl config use-context "$EXPECTED_CONTEXT"
fi
print_status "kubectl context: $EXPECTED_CONTEXT"

# Step 1: Build Maven project
echo ""
print_info "Building Maven project..."
mvn clean package -DskipTests -q
print_status "Maven build complete"

# Step 2: Build Docker image
echo ""
print_info "Building Docker image: $IMAGE_NAME..."
docker build -t "$IMAGE_NAME" . -q
print_status "Docker image built"

# Step 3: Load image into kind cluster
echo ""
print_info "Loading image into kind cluster..."
kind load docker-image "$IMAGE_NAME" --name "$CLUSTER_NAME"
print_status "Image loaded into kind cluster"

# Step 4: Create PVC if needed
echo ""
print_info "Ensuring PVC exists..."
kubectl apply -f k8s/test/pvc-test.yaml 2>/dev/null || true
print_status "PVC ready"

# Step 5: Apply deployment
echo ""
print_info "Applying deployment: $DEPLOYMENT_FILE"
kubectl apply -f "$DEPLOYMENT_FILE"
print_status "Deployment applied"

# Step 6: Wait for job to start
echo ""
print_info "Waiting for job to start..."
sleep 10

# Show status
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Deployment Status${NC}"
echo -e "${BLUE}========================================${NC}"
kubectl get flinkdeployment -n "$NAMESPACE"
echo ""
kubectl get pods -n "$NAMESPACE" | grep -v operator
echo ""

print_status "Deployment complete!"
echo ""
echo "Monitor with:"
echo "  kubectl get flinkdeployment -n $NAMESPACE -w"
echo "  kubectl logs -f -n $NAMESPACE -l component=taskmanager"
echo ""
echo "Operations:"
echo "  Suspend:  ./scripts/stop-resume.sh <job-name> suspend"
echo "  Resume:   ./scripts/stop-resume.sh <job-name> resume"
echo "  Observe:  ./scripts/observe.sh <job-name>"
echo ""
