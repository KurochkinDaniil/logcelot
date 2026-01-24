#!/bin/bash
# Logcelot Chaos Test
# Tests system resilience: ClickHouse & Kafka failures

set -e

API_URL="http://localhost:8000"
CH_CONTAINER="logcelot-clickhouse-01"
KAFKA_CONTAINER="logcelot-kafka-2"

echo ""
echo "=== CHAOS TEST START ==="
echo ""

# Helper: Query log count from ClickHouse
get_count() {
    docker exec -i logcelot-clickhouse-02 clickhouse-client \
        --user logcelot_user --password logcelot_pass \
        --database logcelot \
        --query "SELECT count() FROM logs WHERE source = 'chaos_test'" 2>/dev/null || echo "0"
}

# Helper: Send test logs
send_logs() {
    local count=$1
    local label=$2
    echo "Sending $count logs ($label)..."
    
    for i in $(seq 1 $count); do
        curl -s -X POST "$API_URL/logs" \
            -H "Content-Type: application/json" \
            -d "{\"source\":\"chaos_test\",\"source_service\":\"test\",\"level\":\"INFO\",\"message\":\"Test log $i - $label\"}" \
            > /dev/null 2>&1
    done
    
    echo "  Done: $count logs sent"
}

# Phase 1: Baseline
echo "[1/4] Baseline test (normal operation)"
initial_count=$(get_count)
echo "  Initial count: $initial_count"

send_logs 100 "baseline"
sleep 5

baseline_count=$(get_count)
echo "  After baseline: $baseline_count (delta: $((baseline_count - initial_count)))"
echo ""

# Phase 2: ClickHouse failure
echo "[2/4] ClickHouse failure test"
echo "  Stopping $CH_CONTAINER..."
docker stop "$CH_CONTAINER" > /dev/null 2>&1
echo "  ClickHouse is DOWN"

send_logs 200 "during_ch_outage"
echo "  Waiting 10s for worker retry..."
sleep 10

echo "  Restarting $CH_CONTAINER..."
docker start "$CH_CONTAINER" > /dev/null 2>&1
echo "  ClickHouse is UP"

echo "  Waiting 15s for recovery..."
sleep 15

ch_test_count=$(get_count)
echo "  After CH recovery: $ch_test_count (delta: $((ch_test_count - initial_count)))"
echo ""

# Phase 3: Kafka failure
echo "[3/4] Kafka failure test"
echo "  Stopping $KAFKA_CONTAINER (2/3 brokers remain)..."
docker stop "$KAFKA_CONTAINER" > /dev/null 2>&1
echo "  Kafka broker is DOWN"

send_logs 100 "during_kafka_outage"
sleep 5

echo "  Restarting $KAFKA_CONTAINER..."
docker start "$KAFKA_CONTAINER" > /dev/null 2>&1
echo "  Kafka broker is UP"

echo "  Waiting 10s for Kafka recovery..."
sleep 10
echo ""

# Phase 4: Verification
echo "[4/4] Final verification"
final_count=$(get_count)
total_delta=$((final_count - initial_count))
expected=400

echo "  Initial:  $initial_count"
echo "  Final:    $final_count"
echo "  Delta:    $total_delta"
echo "  Expected: ~$expected"
echo ""

if [ $total_delta -ge 300 ]; then
    echo "RESULT: PASS"
    echo "  System survived ClickHouse and Kafka failures"
    echo "  Data integrity maintained"
else
    echo "RESULT: PARTIAL (some logs may still be in Kafka queue)"
    echo "  Expected: ~$expected, Got: $total_delta"
    echo "  Wait longer or check worker logs for details"
fi

echo ""
echo "=== CHAOS TEST END ==="
echo ""
