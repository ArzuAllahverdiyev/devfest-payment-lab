import json
import os
import time
import uuid
import urllib.request

import psycopg


API_URL = os.getenv(
    "PAYMENT_API_URL",
    "http://payment-api:8000/payments"
)


def publish_payment():
    payment_id = "PAY-STREAM-" + uuid.uuid4().hex[:16].upper()

    payload = {
        "payment_id": payment_id,
        "customer_id": "CUST-STREAM",
        "amount_minor": 10000,
        "currency": "AZN",
    }

    request = urllib.request.Request(
        API_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status != 202:
            raise RuntimeError(f"Unexpected HTTP {response.status}")
        result = json.load(response)

    return payment_id, result["kafka_offset"]


def next_run(conn):
    with conn.transaction():
        row = conn.execute("""
            SELECT run_id, status, rate_per_sec,
                   target_limit, produced_count
            FROM demo.stream_runs
            WHERE status IN (
                'CREATED', 'RUNNING', 'STOP_REQUESTED'
            )
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """).fetchone()

        if row and row[1] == "CREATED":
            conn.execute("""
                UPDATE demo.stream_runs
                SET status = 'RUNNING',
                    started_at = NOW(),
                    updated_at = NOW()
                WHERE run_id = %s
            """, (row[0],))
            row = (row[0], "RUNNING", *row[2:])

    return row


def finish_run(conn, run_id, status):
    conn.execute("""
        UPDATE demo.stream_runs
        SET status = %s,
            finished_at = NOW(),
            updated_at = NOW()
        WHERE run_id = %s
          AND status IN ('RUNNING', 'STOP_REQUESTED')
    """, (status, run_id))

    print(f"[STREAM] {run_id} -> {status}", flush=True)


def main():
    print("[GENERATOR] Started, waiting for stream runs...", flush=True)

    with psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    ) as conn:

        while True:
            run = next_run(conn)

            if run is None:
                time.sleep(1)
                continue

            run_id, status, rate, limit, produced = run

            if status == "STOP_REQUESTED":
                finish_run(conn, run_id, "STOPPED")
                continue

            if produced >= limit:
                finish_run(conn, run_id, "COMPLETED")
                continue

            try:
                payment_id, offset = publish_payment()

                with conn.transaction():
                    result = conn.execute("""
                        UPDATE demo.stream_runs
                        SET produced_count = produced_count + 1,
                            updated_at = NOW()
                        WHERE run_id = %s
                          AND status IN ('RUNNING', 'STOP_REQUESTED')
                        RETURNING produced_count, status
                    """, (run_id,)).fetchone()

                    if result is None:
                        raise RuntimeError("Stream is no longer active")

                    count, current_status = result

                    conn.execute("""
                        INSERT INTO demo.experiment_events
                            (payment_id, component, event_type,
                             message, details)
                        VALUES (
                            %s, 'GENERATOR', 'PUBLISHED',
                            'Payment published to Kafka',
                            %s::jsonb
                        )
                    """, (
                        payment_id,
                        json.dumps({
                            "run_id": str(run_id),
                            "kafka_offset": offset
                        }),
                    ))

                print(
                    f"[STREAM] {count}/{limit} "
                    f"payment={payment_id} offset={offset}",
                    flush=True,
                )

                if count >= limit:
                    finish_run(
                        conn,
                        run_id,
                        "STOPPED" if current_status == "STOP_REQUESTED"
                        else "COMPLETED",
                    )

                time.sleep(1 / rate)

            except Exception as exc:
                conn.execute("""
                    UPDATE demo.stream_runs
                    SET status = 'FAILED',
                        last_error = %s,
                        finished_at = NOW(),
                        updated_at = NOW()
                    WHERE run_id = %s
                      AND status IN ('RUNNING', 'STOP_REQUESTED')
                """, (str(exc)[:1000], run_id))

                print(f"[STREAM ERROR] {exc}", flush=True)
                time.sleep(2)


if __name__ == "__main__":
    main()
