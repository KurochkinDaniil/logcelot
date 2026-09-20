# 🌀 Airflow Setup for Logcelot

## Quick Start

### 1. Start All Services

```bash
# From project root
docker-compose up -d

# Wait for Airflow to initialize (1-2 minutes)
docker-compose logs -f airflow-init
```

### 2. Access Airflow UI

- **URL:** http://localhost:8081
- **Username:** `admin`
- **Password:** `admin`

### 3. Enable DAG

1. Navigate to DAGs page
2. Find `dlq_triage_replay`
3. Toggle switch to **ON**

### 4. Trigger Manual Run

**Via UI:**
- Click ▶️ (Play button) next to DAG
- Select "Trigger DAG"

**Via CLI:**
```bash
docker-compose exec airflow-webserver airflow dags trigger dlq_triage_replay
```

---

## Configuration

### Airflow Variables

Set via UI (`Admin` → `Variables`) or CLI:

```bash
# Example: Enable replay (disable DRY_RUN)
docker-compose exec airflow-webserver \
  airflow variables set DRY_RUN "false"

# Example: Increase sample size
docker-compose exec airflow-webserver \
  airflow variables set DLQ_SAMPLE_SIZE "100"
```

**Available Variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-1:29092,...` | Kafka brokers |
| `DLQ_TOPIC` | `logs.dead_letter` | DLQ topic name |
| `REPLAY_TOPIC` | `logs.raw` | Target topic for replay |
| `DLQ_SAMPLE_SIZE` | `50` | Max messages to sample |
| `DRY_RUN` | `true` | Skip replay if true |
| `CLICKHOUSE_HTTP_URL` | `http://clickhouse-lb:8123` | ClickHouse HTTP endpoint |
| `CLICKHOUSE_DATABASE` | `logcelot` | Database name |
| `CLICKHOUSE_USER` | `logcelot_user` | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | `logcelot_pass` | ClickHouse password |

---

## Monitoring

### DAG Graph View

Shows task dependencies and execution status:

```
dlq_sample_messages → classify_sample → ch_signals → decide_replay
                                                           ├─> maybe_replay ─┐
                                                           └─> skip_replay ───┤
                                                                              ▼
                                                                        final_report
```

### Task Logs

View detailed logs for each task:
1. Click on task node (green/red square)
2. Select "Log"
3. View structured JSON output

### XCom Data

Inspect task outputs:
1. Click on task node
2. Select "XCom"
3. View JSON data passed between tasks

---

## Troubleshooting

### DAG Not Visible

**Symptom:** `dlq_triage_replay` not showing in DAGs list

**Solution:**
```bash
# Check DAG for syntax errors
docker-compose exec airflow-scheduler \
  airflow dags list-import-errors

# View scheduler logs
docker-compose logs airflow-scheduler | grep dlq_triage
```

### Kafka Connection Failed

**Symptom:** Task `dlq_sample_messages` fails with "Connection refused"

**Solution:**
```bash
# Check Kafka health
docker-compose ps | grep kafka

# Verify Kafka topics
docker-compose exec kafka-1 kafka-topics \
  --bootstrap-server localhost:9092 --list

# Check network connectivity
docker-compose exec airflow-webserver ping kafka-1
```

### ClickHouse Connection Failed

**Symptom:** Task `clickhouse_signal_checks` fails with "Connection timeout"

**Solution:**
```bash
# Check ClickHouse health
docker-compose ps | grep clickhouse

# Test connection
docker-compose exec airflow-webserver \
  curl http://clickhouse-lb:8123/ping
```

### Import Errors (Pydantic, Kafka-Python)

**Symptom:** Task fails with `ModuleNotFoundError`

**Solution:**
```bash
# Rebuild Airflow image
docker-compose build airflow-webserver airflow-scheduler

# Restart services
docker-compose restart airflow-webserver airflow-scheduler
```

---

## Development

### Testing DAG Locally

```bash
# Validate DAG structure
docker-compose exec airflow-scheduler \
  python /opt/airflow/dags/dlq_triage_replay_dag.py

# Test specific task
docker-compose exec airflow-scheduler \
  airflow tasks test dlq_triage_replay classify_sample 2024-01-20
```

### Viewing Logs

```bash
# Scheduler logs
docker-compose logs -f airflow-scheduler

# Webserver logs
docker-compose logs -f airflow-webserver

# Task logs (stored in volume)
docker-compose exec airflow-webserver \
  cat /opt/airflow/logs/dlq_triage_replay/dlq_sample_messages/2024-01-20T10:00:00+00:00/1.log
```

### Rebuilding DAG

After making changes to DAG files:

```bash
# Airflow auto-detects changes (no restart needed)
# Wait ~30 seconds for scheduler to pick up changes

# Or force DAG refresh
docker-compose exec airflow-scheduler \
  airflow dags reserialize dlq_triage_replay
```

---

## Architecture Notes

### Why Airflow? (For Diploma Defense)

**This is NOT Lambda Architecture!**

- **Real-time path:** Kafka → Consumer → ClickHouse (streaming)
- **Batch path (Airflow):** DLQ monitoring, triage, recovery (operational tasks)

**Airflow handles:**
- ✅ Periodic DLQ monitoring (every 5 min)
- ✅ Error classification and reporting
- ✅ Auto-recovery of transient failures
- ✅ Data quality checks (parse errors, lag)

**Airflow does NOT:**
- ❌ Duplicate real-time ingestion
- ❌ Replace Kafka streaming
- ❌ Process all logs (only DLQ)

---

## References

- [Full Documentation](../../docs/AIRFLOW_PIPELINE.md)
- [DAG Source Code](dags/dlq_triage_replay_dag.py)
- [Unit Tests](../../tests/unit/test_dlq_classifier.py)
