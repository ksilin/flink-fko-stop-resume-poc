#!/bin/bash

# Flink Test Cluster Setup Script
# Sets up a kind cluster with Flink Kubernetes Operator

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CLUSTER_NAME=${CLUSTER_NAME:-flink-test}
FLINK_OPERATOR_VERSION=${FLINK_OPERATOR_VERSION:-1.10.0}
CERT_MANAGER_VERSION=${CERT_MANAGER_VERSION:-v1.8.2}

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Flink Test Cluster Setup (kind)${NC}"
echo -e "${BLUE}========================================${NC}"

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to print status
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${YELLOW}➜${NC} $1"
}

# Check prerequisites
echo ""
print_info "Checking prerequisites..."

MISSING_DEPS=()

if ! command_exists kind; then
    print_error "kind not found"
    MISSING_DEPS+=("kind")
else
    print_status "kind found: $(kind version)"
fi

if ! command_exists kubectl; then
    print_error "kubectl not found"
    MISSING_DEPS+=("kubectl")
else
    print_status "kubectl found: $(kubectl version --client --short 2>/dev/null || kubectl version --client -o json | grep gitVersion | head -1)"
fi

if ! command_exists helm; then
    print_error "helm not found"
    MISSING_DEPS+=("helm")
else
    print_status "helm found: $(helm version --short)"
fi

if ! command_exists docker; then
    print_error "docker not found"
    MISSING_DEPS+=("docker")
else
    print_status "docker found: $(docker --version)"
fi

if [ ${#MISSING_DEPS[@]} -ne 0 ]; then
    echo ""
    print_error "Missing dependencies: ${MISSING_DEPS[*]}"
    echo ""
    echo "Please install missing dependencies:"
    echo "  - kind: https://kind.sigs.k8s.io/docs/user/quick-start/#installation"
    echo "  - kubectl: https://kubernetes.io/docs/tasks/tools/"
    echo "  - helm: https://helm.sh/docs/intro/install/"
    echo "  - docker: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker is running
if ! docker info >/dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi
print_status "Docker is running"

echo ""
print_info "Creating kind cluster: $CLUSTER_NAME"

# Check if cluster already exists
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    print_info "Cluster '$CLUSTER_NAME' already exists. Deleting..."
    kind delete cluster --name "$CLUSTER_NAME"
fi

# Create kind cluster
kind create cluster --name "$CLUSTER_NAME"

print_status "Kind cluster created"

# Set kubectl context
kubectl config use-context "kind-${CLUSTER_NAME}"
print_status "kubectl context set to kind-${CLUSTER_NAME}"

# Verify cluster is ready
echo ""
print_info "Waiting for cluster to be ready..."
kubectl wait --for=condition=Ready nodes --all --timeout=300s

print_status "Cluster is ready"

# Install cert-manager
echo ""
print_info "Installing cert-manager ${CERT_MANAGER_VERSION}..."

kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml

print_info "Waiting for cert-manager to be ready..."
sleep 10  # Give it time to create resources
kubectl wait --for=condition=Available --timeout=300s \
    -n cert-manager deployment/cert-manager \
    deployment/cert-manager-webhook \
    deployment/cert-manager-cainjector

print_status "cert-manager installed successfully"

# Create flink namespace
echo ""
print_info "Creating flink namespace..."
kubectl create namespace flink --dry-run=client -o yaml | kubectl apply -f -
print_status "Namespace 'flink' created"

# Install Flink Kubernetes Operator
echo ""
print_info "Installing Flink Kubernetes Operator ${FLINK_OPERATOR_VERSION}..."

# Add Flink operator repo
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/ 2>/dev/null || true
helm repo update

# Install operator in flink namespace, watching flink namespace
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
    --namespace flink \
    --set watchNamespaces='{flink}' \
    --wait \
    --timeout=300s

print_status "Flink Kubernetes Operator installed"

# Verify installation
echo ""
print_info "Verifying installation..."

# Wait for operator pod
kubectl wait --for=condition=Ready \
    -n flink \
    -l app.kubernetes.io/name=flink-kubernetes-operator \
    pod \
    --timeout=300s

print_status "Flink Kubernetes Operator is running"

# Display cluster info
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Cluster Setup Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Cluster Information:"
echo "  Kind Cluster: ${CLUSTER_NAME}"
echo "  kubectl Context: kind-${CLUSTER_NAME}"
echo "  Flink Operator: ${FLINK_OPERATOR_VERSION}"
echo "  Cert Manager: ${CERT_MANAGER_VERSION}"
echo ""
echo "Namespaces:"
kubectl get namespace cert-manager flink --no-headers
echo ""
echo "Operator Pod:"
kubectl get pods -n flink -l app.kubernetes.io/name=flink-kubernetes-operator
echo ""
echo "Service Accounts:"
kubectl get serviceaccount -n flink
echo ""
print_status "Setup complete!"
echo ""
echo "Next steps:"
echo "  1. Build and deploy Flink jobs: ./scripts/deploy.sh"
echo "  2. Or deploy manually:"
echo "     docker build -t flink-deployment-test:latest ."
echo "     kind load docker-image flink-deployment-test:latest --name ${CLUSTER_NAME}"
echo "     kubectl apply -f k8s/test/stateful-test-pvc.yaml"
echo ""
echo "To delete the cluster:"
echo "  ./scripts/teardown-cluster.sh"
echo ""
