# consumer/main.py

import os
import time
from datetime import datetime, timezone

import psycopg2
import redis
from dotenv import load_dotenv

load_dotenv()

STREAM_NAME = "freight-events"
GROUP_NAME = "freightpilot-consumers"
CONSUMER_NAME = "consumer-1"
METRICS_INTERVAL = 30  # seconds

REQUIRED_FIELDS = {"shipment_id", "event_type", "mode", "timestamp"}

STATUS_MAP = {
    "shipment_created": None,
    "picked_up": "picked_up",
    "in_transit": "in_transit",
    "out_for_delivery": "out_for_delivery",
    "delivered": "delivered",
    "delay_reported": "delayed",
    "carrier_silent": "in_transit",
    "location_update": None,  # no status change
}


def create_consumer_group(r):
    try:
        r.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        print(f"[consumer] Created consumer group '{GROUP_NAME}'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"[consumer] Consumer group '{GROUP_NAME}' already exists")
        else:
            raise


def process_message(conn, message_id, data):
    # Validate required fields
    if not REQUIRED_FIELDS.issubset(data.keys()):
        missing = REQUIRED_FIELDS - data.keys()
        print(f"[consumer] Skipping {message_id} — missing fields: {missing}")
        return False

    shipment_id = data["shipment_id"]
    event_type = data["event_type"]
    mode = data["mode"]
    location = data.get("location")
    note = data.get("note")
    timestamp_str = data["timestamp"]

    try:
        timestamp = datetime.fromisoformat(timestamp_str)
    except ValueError:
        print(f"[consumer] Skipping {message_id} — invalid timestamp: {timestamp_str}")
        return False

    new_status = STATUS_MAP.get(event_type)
    try:
        with conn.cursor() as cur:
            # Insert event
            cur.execute(
                """INSERT INTO events (shipment_id, event_type, mode, location, note, timestamp, stream_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (shipment_id, event_type, mode, location, note, timestamp, message_id)
            )
            # Update shipment last_update
            cur.execute(
                "UPDATE shipments SET last_update = NOW() WHERE id = %s",
                (shipment_id,)
            )
            # Update status if applicable
            if new_status is not None:
                cur.execute(
                    "UPDATE shipments SET status = %s WHERE id = %s",
                    (new_status, shipment_id)
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[consumer] Transaction rolled back for {message_id}: {e}")
        return False
    return True


def insert_metrics(conn, r, events_processed, latencies):
    now = datetime.now(timezone.utc)
    events_per_second = events_processed / METRICS_INTERVAL if METRICS_INTERVAL > 0 else 0

    try:
        pending_info = r.xpending(STREAM_NAME, GROUP_NAME)
        consumer_lag = pending_info.get("pending", 0) if isinstance(pending_info, dict) else pending_info[0]
    except Exception:
        consumer_lag = 0

    avg_latency = (sum(latencies) / len(latencies)) if latencies else 0.0

    with conn.cursor() as cur:
        cur.execute(
            """SELECT
               COUNT(*) FILTER (
                 WHERE carrier_status = 'unresponsive'
                    OR (eta_hours IS NOT NULL AND last_update < NOW() - INTERVAL '1 hour' * (eta_hours - 4) AND eta_hours > 4)
                    OR is_anomaly = TRUE
               ) AS exceptions_detected,
               COUNT(*) FILTER (WHERE is_anomaly = TRUE) AS anomalies_detected
               FROM shipments
               WHERE status NOT IN ('delivered', 'resolved')"""
        )
        row = cur.fetchone()
        exceptions_detected = row[0]
        anomalies_detected  = row[1]

        cur.execute(
            """INSERT INTO pipeline_metrics
               (timestamp, events_processed, events_per_second, consumer_lag,
                exceptions_detected, anomalies_detected, processing_latency_ms)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (now, events_processed, events_per_second, consumer_lag,
             exceptions_detected, anomalies_detected, avg_latency)
        )
    conn.commit()
    print(
        f"[consumer] Metrics — processed: {events_processed}, "
        f"eps: {events_per_second:.2f}, lag: {consumer_lag}, "
        f"exceptions: {exceptions_detected}, anomalies: {anomalies_detected}, "
        f"latency: {avg_latency:.1f}ms"
    )


def main():
    r = redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    conn = psycopg2.connect(os.environ["DATABASE_URL"])

    print("[consumer] Connected to Redis and PostgreSQL")
    create_consumer_group(r)

    events_processed = 0
    latencies = []
    last_metrics_time = time.time()

    try:
        while True:
            messages = r.xreadgroup(
                GROUP_NAME, CONSUMER_NAME,
                {STREAM_NAME: ">"},
                count=10,
                block=5000
            )

            if messages:
                for stream, entries in messages:
                    for message_id, data in entries:
                        t_start = time.time()
                        ok = process_message(conn, message_id, data)
                        elapsed_ms = (time.time() - t_start) * 1000

                        if ok:
                            events_processed += 1
                            latencies.append(elapsed_ms)
                            print(f"[consumer] ✓ {data.get('event_type')} for {data.get('shipment_id')} ({elapsed_ms:.1f}ms)")

                        # Acknowledge regardless (skip bad messages)
                        r.xack(STREAM_NAME, GROUP_NAME, message_id)

            # Emit metrics every 30s
            if time.time() - last_metrics_time >= METRICS_INTERVAL:
                insert_metrics(conn, r, events_processed, latencies)
                events_processed = 0
                latencies = []
                last_metrics_time = time.time()

    except KeyboardInterrupt:
        print("[consumer] Shutting down gracefully")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
