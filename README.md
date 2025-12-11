# Flink Deployment Test Project

Test project for validating Flink stop/resume behavior with the Flink Kubernetes Operator across different upgrade modes and configurations.

## Status

| Maven Build | Flink 1.20.3 |
| Docker Build | flink:1.20-java11 |
| K8s Deployment | kind cluster |
| Kafka Integration | ⚠️ |

**Platform**: kind (Kubernetes IN Docker)
**Last Updated**: 2025-12-09

## Quick Start

### Prerequisites

**Required:**
- Docker
- kind (`brew install kind`)
- kubectl
- helm
- Java 11+ and Maven
- Python 3.10+ (for test framework)

**Python Dependencies:**
```bash
pip install -r tests/requirements.txt
```

### 1. Setup Cluster

```bash
./scripts/setup-cluster.sh
```

This creates a kind cluster with:
- Flink Kubernetes Operator 1.10.0
- cert-manager
- flink namespace with service accounts

### 2. Setup Storage (Critical!)

Create PersistentVolumes and PersistentVolumeClaims for state storage:

```bash
# Create PVs and PVCs
kubectl apply -f k8s/base/pv-savepoints.yaml
kubectl apply -f k8s/base/pv-checkpoints.yaml
kubectl apply -f k8s/base/pvc-savepoints.yaml
kubectl apply -f k8s/base/pvc-checkpoints.yaml

# Fix permissions on kind node
docker exec flink-test-control-plane bash -c \
  "mkdir -p /mnt/flink-savepoints /mnt/flink-checkpoints && \
   chmod 777 /mnt/flink-savepoints /mnt/flink-checkpoints"
```

**Why this is needed:**
- Stateful jobs require PersistentVolumes for savepoints and checkpoints
- ReadWriteMany (RWX) access mode allows multiple pods to share volumes
- Directory permissions must allow Flink user to write state

### 3. Build and Deploy

```bash
./scripts/deploy.sh
```

Or manually:
```bash
# Build JAR
mvn clean package -DskipTests

# Build Docker image
docker build -t flink-deployment-test:latest .

# Load into kind
kind load docker-image flink-deployment-test:latest --name flink-test

# Deploy test job (optional - tests will deploy dynamically)
kubectl apply -f k8s/test/pvc-test.yaml
kubectl apply -f k8s/test/stateful-test-pvc.yaml
```

### 4. Run Automated Tests

#### Basic Test Execution

```bash
# Run all test scenarios
pytest tests/scenarios -v

# Run specific test file
pytest tests/scenarios/test_stateless.py -v
pytest tests/scenarios/test_stateful_savepoint.py -v

# Run specific test
pytest "tests/scenarios/test_stateful_savepoint.py::test_stateful_savepoint_basic_suspend_resume" -v
```

#### Test Categories

**Stateless Job Tests** (~2 minutes):
```bash
pytest tests/scenarios/test_stateless.py -v
```
- Tests suspend/resume without state preservation
- Validates job restarts cleanly

**Stateful Savepoint Tests** (~5 minutes):
```bash
pytest tests/scenarios/test_stateful_savepoint.py -v
```
- Tests savepoint creation and restoration
- Tests watermark configuration changes during suspend/resume
- Validates state preservation

**Drain Behavior Tests** (~2 minutes):
```bash
pytest tests/scenarios/test_draining.py::test_drain_enabled_emits_windows_on_suspend -v
```
- Tests window emission on job suspension
- Validates `kubernetes.operator.job.drain-on-savepoint-deletion` behavior

**Upgrade Modes Matrix Tests** (~18 minutes for all):
```bash
# Test all upgrade modes and watermark combinations
pytest tests/scenarios/test_upgrade_modes_matrix.py::test_stateful_suspend_resume_matrix -v

# Test specific upgrade mode
pytest "tests/scenarios/test_upgrade_modes_matrix.py::test_stateful_suspend_resume_matrix[savepoint-True]" -v
```

Matrix dimensions:
- **Upgrade modes**: savepoint, last-state, stateless
- **Watermarks**: enabled (True), disabled (False)
- **Total**: 6 test combinations

#### Test Options

```bash
# Show detailed output
pytest tests/scenarios/test_stateless.py -v -s

# Run with shorter timeout (default: 600s)
pytest tests/scenarios/test_stateless.py --timeout=300

# Show test durations
pytest tests/scenarios -v --durations=10

# Run tests matching pattern
pytest tests/scenarios -k "savepoint" -v
```

### 5. Manual Testing (Interactive)

Use the Python CLI for manual operations:

```bash
# Install dependencies
pip install -r tests/requirements.txt

# List deployments
python tests/cli.py list

# Suspend (creates savepoint for stateful jobs)
python tests/cli.py suspend stateful-test --wait

# Resume (restores from savepoint)
python tests/cli.py resume stateful-test --wait

# Toggle watermarks
python tests/cli.py watermarks stateful-test --enable
python tests/cli.py watermarks stateful-test --disable

# View status and logs
python tests/cli.py status stateful-test
python tests/cli.py logs stateful-test --lines 50
```

Or use shell scripts:
```bash
./scripts/stop-resume.sh stateful-test suspend
./scripts/stop-resume.sh stateful-test resume
./scripts/observe.sh stateful-test
```

### 6. Cleanup

```bash
# Delete all FlinkDeployments
kubectl delete flinkdeployment --all -n flink

# Delete cluster
./scripts/teardown-cluster.sh
```

## Project Structure

```
├── src/main/java/com/example/flink/
│   ├── StatefulJob.java           # Keyed state with checkpointing
│   ├── StatelessJob.java          # Simple stateless processing
│   ├── WindowedDrainTestJob.java  # Windowed aggregation for drain tests
│   ├── KafkaStatefulJob.java      # Kafka + state (not yet tested)
│   └── KafkaStatelessJob.java     # Kafka streaming (not yet tested)
├── k8s/
│   ├── base/                      # Base Kubernetes resources
│   │   ├── pv-savepoints.yaml    # PersistentVolume for savepoints
│   │   ├── pv-checkpoints.yaml   # PersistentVolume for checkpoints
│   │   ├── pvc-savepoints.yaml   # PVC (RWX, 10Gi)
│   │   └── pvc-checkpoints.yaml  # PVC (RWX, 10Gi)
│   ├── test/                      # Simple test deployments
│   │   ├── stateful-test-pvc.yaml
│   │   ├── stateless-test.yaml
│   │   └── pvc-test.yaml
│   └── deployments/               # Full deployment configurations
│       ├── stateful-savepoint.yaml     # upgradeMode: savepoint
│       ├── stateful-laststate.yaml     # upgradeMode: last-state
│       ├── stateful-stateless-mode.yaml # upgradeMode: stateless
│       ├── drain-test-*.yaml           # Drain behavior configs
│       └── kafka-stateful-*.yaml       # Kafka variants
├── scripts/                       # Infrastructure scripts
│   ├── setup-cluster.sh          # Create kind cluster + operator
│   ├── teardown-cluster.sh       # Delete cluster
│   └── deploy.sh                 # Build + load + deploy
└── tests/                        # Python test framework
    ├── requirements.txt          # pytest, pyyaml, kubernetes
    ├── pytest.ini                # Test configuration
    ├── conftest.py               # Shared fixtures
    ├── cli.py                    # Manual operations CLI
    ├── framework/                # Reusable components
    │   ├── kubernetes_client.py  # K8s API wrapper
    │   ├── flink_deployment.py   # FlinkDeployment manager
    │   └── log_parser.py         # Log parsing utilities
    └── scenarios/                # Automated test scenarios
        ├── test_stateless.py              # Stateless suspend/resume
        ├── test_stateful_savepoint.py     # Stateful with savepoint
        ├── test_draining.py               # Drain behavior tests
        ├── test_upgrade_modes_matrix.py   # Comprehensive matrix tests
        └── test_kafka_stateful.py         # Kafka integration (WIP)
```

## Jobs

### StatefulJob
- **Purpose**: Validates state preservation across suspend/resume
- **Data**: Generates keyed messages (key-0 through key-9)
- **State**: Per-key counter in `ValueState<Integer>`
- **Checkpointing**: Every 10 seconds
- **Output format**: `key-0|count=100|index=123|processedAt=1734621234567`
- **Watermarks**: Configurable via `--watermarks=true/false`
- **Rate**: ~1 message/second per key (10 total/sec)

**Log indicators:**
- `First message for key: key-0` - State is fresh (null)
- `State validation - Key: key-0, Count: 100` - Logged every 100 messages

### StatelessJob
- **Purpose**: Baseline for jobs without state
- **Data**: Sequential messages
- **State**: None
- **Checkpointing**: Disabled
- **Rate**: ~10 messages/second
- **Watermarks**: Configurable via `--watermarks=true/false`

### WindowedDrainTestJob
- **Purpose**: Tests window emission on job drain
- **Windows**: 30-second tumbling windows
- **State**: Windowed aggregations
- **Drain behavior**: Emits incomplete windows when `drain-on-savepoint-deletion` is enabled

## Test Scenarios

### Test: Stateless Suspend/Resume
**File**: `test_stateless.py`
**Duration**: ~95 seconds
**Validates**:
- Job deploys and reaches STABLE
- Suspend transitions to SUSPENDED
- Resume returns to STABLE
- No state preservation (expected)

### Test: Stateful Savepoint Operations
**File**: `test_stateful_savepoint.py`
**Duration**: ~3-5 minutes
**Validates**:
- Savepoint creation on suspend
- State restoration on resume
- Counters continue from previous values
- Watermark configuration changes work across suspend/resume

### Test: Drain Behavior
**File**: `test_draining.py`
**Duration**: ~2 minutes
**Validates**:
- Windows emit on suspend when drain is enabled
- Watermark advances to MAX_VALUE (9223372036854775807)
- Incomplete windows are flushed
- No data loss during drain

### Test: Upgrade Modes Matrix
**File**: `test_upgrade_modes_matrix.py`
**Duration**: ~18 minutes (6 tests)
**Validates**:

| Upgrade Mode | Watermarks | State Behavior | Verification Method |
|--------------|------------|----------------|---------------------|
| savepoint | True | PRESERVED | Counter values continue |
| savepoint | False | PRESERVED | Counter values continue |
| last-state | True | PRESERVED | Counter values continue |
| last-state | False | PRESERVED | Counter values continue |
| stateless | True | RESET | "First message" logs detected |
| stateless | False | RESET | "First message" logs detected |

**Detection methods** (ordered by reliability):
1. **Confirmed**: "First message for key" logs present
2. **Inferred**: Minimum counter values < 10
3. **Likely**: Average counter in expected range (35-80)

## Configuration

### Environment Variables
- `CLUSTER_NAME`: kind cluster name (default: `flink-test`)
- `NAMESPACE`: Kubernetes namespace (default: `flink`)
- `IMAGE_NAME`: Docker image name (default: `flink-deployment-test:latest`)

### Pytest Configuration
Edit `tests/pytest.ini` to adjust:
- `timeout = 600` - Test timeout in seconds
- `log_cli_level = INFO` - Log verbosity
- `asyncio_default_test_loop_scope = function` - Async test scope

### Job Configuration
All jobs support:
- `--watermarks=true|false` - Enable/disable watermark generation