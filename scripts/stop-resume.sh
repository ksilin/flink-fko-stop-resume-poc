#!/bin/bash

# Stop/Resume Flink Deployment
# Controls job lifecycle and configuration
#
# DEPRECATION NOTICE: This script is being replaced by the Python CLI.
# Prefer using: python tests/cli.py <command> <name>
#
# Equivalent commands:
#   ./scripts/stop-resume.sh name suspend     → python tests/cli.py suspend name
#   ./scripts/stop-resume.sh name resume      → python tests/cli.py resume name
#   ./scripts/stop-resume.sh name watermarks-on  → python tests/cli.py watermarks name --enable
#   ./scripts/stop-resume.sh name watermarks-off → python tests/cli.py watermarks name --disable

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <deployment-name> <action> [namespace]"
    echo ""
    echo "Actions:"
    echo "  suspend        - Stop the job (creates savepoint for stateful jobs)"
    echo "  resume         - Start/resume the job"
    echo "  watermarks-on  - Enable watermarks and restart"
    echo "  watermarks-off - Disable watermarks and restart"
    echo ""
    echo "Examples:"
    echo "  $0 stateful-test suspend"
    echo "  $0 stateful-test resume flink"
    echo "  $0 stateful-test watermarks-on"
    exit 1
fi

DEPLOYMENT_NAME=$1
ACTION=$2
NAMESPACE=${3:-flink}

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}➜${NC} $ACTION on $DEPLOYMENT_NAME (namespace: $NAMESPACE)"

case $ACTION in
    suspend)
        echo "Suspending job: $DEPLOYMENT_NAME"
        kubectl patch flinkdeployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" \
            --type merge -p '{"spec":{"job":{"state":"suspended"}}}'
        ;;
    resume)
        echo "Resuming job: $DEPLOYMENT_NAME"
        kubectl patch flinkdeployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" \
            --type merge -p '{"spec":{"job":{"state":"running"}}}'
        ;;
    watermarks-on)
        echo "Enabling watermarks for: $DEPLOYMENT_NAME"
        kubectl patch flinkdeployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" \
            --type merge -p '{"spec":{"job":{"args":["--watermarks=true"]}}}'
        ;;
    watermarks-off)
        echo "Disabling watermarks for: $DEPLOYMENT_NAME"
        kubectl patch flinkdeployment "$DEPLOYMENT_NAME" -n "$NAMESPACE" \
            --type merge -p '{"spec":{"job":{"args":["--watermarks=false"]}}}'
        ;;
    *)
        echo "Unknown action: $ACTION"
        echo "Valid actions: suspend, resume, watermarks-on, watermarks-off"
        exit 1
        ;;
esac

echo ""
echo "Waiting for change to apply..."
sleep 3

echo ""
echo -e "${GREEN}✓${NC} Current status:"
kubectl get flinkdeployment "$DEPLOYMENT_NAME" -n "$NAMESPACE"
