#!/bin/bash

# Observe Flink Deployment Status
# Shows detailed information about a Flink deployment
#
# DEPRECATION NOTICE: This script is being replaced by the Python CLI.
# Prefer using: python tests/cli.py status <name>
#
# Equivalent commands:
#   ./scripts/observe.sh name           → python tests/cli.py status name
#   ./scripts/observe.sh name -v        → python tests/cli.py status name --verbose
#   (view logs)                         → python tests/cli.py logs name

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <deployment-name> [namespace]"
    echo ""
    echo "Examples:"
    echo "  $0 stateful-test"
    echo "  $0 stateful-test flink"
    exit 1
fi

DEPLOYMENT_NAME=$1
NAMESPACE=${2:-flink}

# Color codes
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== FlinkDeployment Status ===${NC}"
kubectl get flinkdeployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" -o wide 2>/dev/null || {
    echo "Deployment '$DEPLOYMENT_NAME' not found in namespace '$NAMESPACE'"
    echo ""
    echo "Available deployments:"
    kubectl get flinkdeployment -n "$NAMESPACE" 2>/dev/null || echo "  (none)"
    exit 1
}

echo ""
echo -e "${BLUE}=== Job Details ===${NC}"
kubectl get flinkdeployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" -o jsonpath='{.status}' 2>/dev/null | jq . 2>/dev/null || \
    kubectl get flinkdeployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" -o jsonpath='{.status}'

echo ""
echo -e "${BLUE}=== Pods ===${NC}"
kubectl get pods -n "$NAMESPACE" -l "app=$DEPLOYMENT_NAME"

echo ""
echo -e "${BLUE}=== Recent Events ===${NC}"
kubectl get events -n "$NAMESPACE" --field-selector "involvedObject.name=$DEPLOYMENT_NAME" --sort-by='.lastTimestamp' 2>/dev/null | tail -10 || \
    echo "(no events)"

echo ""
echo -e "${BLUE}=== JobManager Logs (last 20 lines) ===${NC}"
JM_POD=$(kubectl get pods -n "$NAMESPACE" -l "app=$DEPLOYMENT_NAME" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null | head -1)
if [ -n "$JM_POD" ]; then
    kubectl logs "$JM_POD" -n "$NAMESPACE" --tail=20 2>/dev/null || echo "(logs unavailable)"
else
    echo "No JobManager pod found"
fi

echo ""
echo -e "${BLUE}=== TaskManager Output (last 10 lines) ===${NC}"
TM_POD=$(kubectl get pods -n "$NAMESPACE" -l "app=$DEPLOYMENT_NAME,component=taskmanager" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$TM_POD" ]; then
    kubectl logs "$TM_POD" -n "$NAMESPACE" --tail=10 2>/dev/null || echo "(logs unavailable)"
else
    # Try alternative selector
    TM_POD=$(kubectl get pods -n "$NAMESPACE" | grep "$DEPLOYMENT_NAME-taskmanager" | head -1 | awk '{print $1}')
    if [ -n "$TM_POD" ]; then
        kubectl logs "$TM_POD" -n "$NAMESPACE" --tail=10 2>/dev/null || echo "(logs unavailable)"
    else
        echo "No TaskManager pod found"
    fi
fi
