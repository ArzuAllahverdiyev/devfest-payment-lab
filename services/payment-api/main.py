import json
import os
import threading

from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer

from shared.payment_schema import (
    PaymentRequest,
    PaymentRequestedEvent,
)

app = FastAPI(title="DevFest Payment API")

producer = Producer({
    "bootstrap.servers": os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
    ),
    "enable.idempotence": True,
    "acks": "all",
})

producer_lock = threading.Lock()


@app.get("/health")
def health():
    return {"status": "healthy", "service": "payment-api"}


@app.post("/payments", status_code=202)
def create_payment(request: PaymentRequest):

    event = PaymentRequestedEvent(**request.model_dump())

    payload = event.model_dump(mode="json")
    delivery_errors = []
    delivered = []

    def delivery_report(err, msg):
        if err:
            delivery_errors.append(str(err))
        else:
            delivered.append(msg.offset())

    try:
        with producer_lock:
            producer.produce(
                topic="payment.requests",
                key=event.payment_id.encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                on_delivery=delivery_report,
            )

            remaining = producer.flush(timeout=10)

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Kafka publish outcome uncertain: {exc}",
        )

    if remaining or delivery_errors or not delivered:
        raise HTTPException(
            status_code=503,
            detail="Kafka delivery not confirmed. Retry with the same payment_id.",
        )

    return {
        "payment_id": event.payment_id,
        "event_id": event.event_id,
        "status": "ACCEPTED",
        "kafka_offset": delivered[0],
    }


# Dashboard read API
import psycopg


@app.get("/api/payments/{payment_id}")
def get_payment_details(payment_id: str):
    with psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ) as conn:

        payment = conn.execute("""
            SELECT payment_id, amount_minor, currency, status
            FROM app.payments
            WHERE payment_id = %s
        """, (payment_id,)).fetchone()

        if payment is None:
            raise HTTPException(404, "Payment not found")

        charges = conn.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount_minor), 0)
            FROM gateway.gateway_charges
            WHERE payment_id = %s
        """, (payment_id,)).fetchone()

        inbox = conn.execute("""
            SELECT status, attempts
            FROM app.consumer_inbox
            WHERE payment_id = %s
            ORDER BY created_at DESC
            LIMIT 1
        """, (payment_id,)).fetchone()

    return {
        "payment_id": payment[0],
        "expected_amount_minor": payment[1],
        "currency": payment[2],
        "payment_status": payment[3],
        "charge_count": charges[0],
        "actual_amount_minor": charges[1],
        "duplicate_count": max(0, charges[0] - 1),
        "inbox_status": inbox[0] if inbox else None,
        "processing_attempts": inbox[1] if inbox else 0,
    }

from fastapi.responses import FileResponse

@app.get("/")
def dashboard():
    return FileResponse("/app/ui/index.html")


@app.get("/api/experiments/{payment_id}/events")
def get_experiment_events(payment_id: str):
    with psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    ) as conn:

        control = conn.execute(
            """
            SELECT mode, action, phase
            FROM demo.experiment_controls
            WHERE payment_id = %s
            """,
            (payment_id,),
        ).fetchone()

        if control is None:
            raise HTTPException(
                status_code=404,
                detail="Experiment not found"
            )

        events = conn.execute(
            """
            SELECT id, component, event_type, message, created_at
            FROM demo.experiment_events
            WHERE payment_id = %s
            ORDER BY id DESC
            LIMIT 100
            """,
            (payment_id,),
        ).fetchall()

    return {
        "payment_id": payment_id,
        "mode": control[0],
        "action": control[1],
        "phase": control[2],
        "events": [
            {
                "id": row[0],
                "component": row[1],
                "event_type": row[2],
                "message": row[3],
                "timestamp": row[4].isoformat(),
            }
            for row in reversed(events)
        ],
    }


# DevFest Streaming Control API
from uuid import uuid4
from pydantic import BaseModel, Field
from confluent_kafka import Consumer as MetricsConsumer, TopicPartition


class StreamStartRequest(BaseModel):
    rate_per_sec: int = Field(default=2, ge=1, le=5)
    target_limit: int = Field(default=100, ge=1, le=300)


def stream_connection():
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


@app.post("/api/streams/start", status_code=201)
def start_stream(request: StreamStartRequest):
    run_id = uuid4()
    try:
        with stream_connection() as conn:
            conn.execute("""
                INSERT INTO demo.stream_runs
                    (run_id, status, rate_per_sec, target_limit)
                VALUES (%s, 'CREATED', %s, %s)
            """, (
                run_id,
                request.rate_per_sec,
                request.target_limit,
            ))
    except psycopg.errors.UniqueViolation:
        raise HTTPException(
            status_code=409,
            detail="Another stream is already active",
        )

    return {"run_id": str(run_id), "status": "CREATED"}


@app.post("/api/streams/stop")
def stop_stream():
    with stream_connection() as conn:
        row = conn.execute("""
            UPDATE demo.stream_runs
            SET status = 'STOP_REQUESTED',
                updated_at = NOW()
            WHERE status IN ('CREATED', 'RUNNING')
            RETURNING run_id
        """).fetchone()

    if row is None:
        return {"status": "NO_ACTIVE_STREAM"}

    return {
        "run_id": str(row[0]),
        "status": "STOP_REQUESTED",
    }


def kafka_metrics():
    consumer = None

    try:
        consumer = MetricsConsumer({
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
            ),
            "group.id": "devfest-payment-worker",
            "enable.auto.commit": False,
        })

        partition = TopicPartition("payment.requests", 0)

        committed = consumer.committed(
            [partition], timeout=3
        )[0].offset

        low, high = consumer.get_watermark_offsets(
            partition, timeout=3, cached=False
        )

        effective_offset = committed if committed >= 0 else low

        return {
            "committed_offset": effective_offset,
            "log_end_offset": high,
            "lag": max(0, high - effective_offset),
        }
    except Exception as exc:
        return {
            "committed_offset": None,
            "log_end_offset": None,
            "lag": None,
            "error": str(exc)[:200],
        }
    finally:
        if consumer is not None:
            consumer.close()


@app.get("/api/streams/status")
def stream_status():
    with stream_connection() as conn:
        row = conn.execute("""
            SELECT run_id, status, rate_per_sec,
                   target_limit, produced_count,
                   created_at, started_at, finished_at,
                   last_error
            FROM demo.stream_runs
            ORDER BY created_at DESC
            LIMIT 1
        """).fetchone()

        run = None
        recent = []

        if row:
            run_id = str(row[0])

            processed = conn.execute("""
                SELECT COUNT(*)
                FROM demo.experiment_events e
                JOIN app.payments p
                  ON p.payment_id = e.payment_id
                WHERE e.component = 'GENERATOR'
                  AND e.event_type = 'PUBLISHED'
                  AND e.details->>'run_id' = %s
                  AND p.status = 'SUCCEEDED'
            """, (run_id,)).fetchone()[0]

            recent_rows = conn.execute("""
                SELECT e.payment_id,
                       e.details->>'kafka_offset',
                       COALESCE(p.status, 'QUEUED')
                FROM demo.experiment_events e
                LEFT JOIN app.payments p
                  ON p.payment_id = e.payment_id
                WHERE e.component = 'GENERATOR'
                  AND e.event_type = 'PUBLISHED'
                  AND e.details->>'run_id' = %s
                ORDER BY e.id DESC
                LIMIT 8
            """, (run_id,)).fetchall()

            recent = [
                {
                    "payment_id": item[0],
                    "offset": item[1],
                    "status": item[2],
                }
                for item in recent_rows
            ]

            run = {
                "run_id": run_id,
                "status": row[1],
                "rate_per_sec": row[2],
                "target_limit": row[3],
                "produced_count": row[4],
                "processed_count": processed,
                "created_at": row[5].isoformat(),
                "started_at": (
                    row[6].isoformat() if row[6] else None
                ),
                "finished_at": (
                    row[7].isoformat() if row[7] else None
                ),
                "last_error": row[8],
            }

    return {
        "run": run,
        "recent_payments": recent,
        "kafka": kafka_metrics(),
    }


# UI-controlled consumer supervisor
@app.post("/api/worker/crash")
def crash_worker():
    with stream_connection() as conn:
        conn.execute("""
            UPDATE demo.worker_runtime
            SET desired_state = 'STOPPED', updated_at = NOW()
            WHERE id = 1
        """)
    return {"requested_state": "STOPPED"}


@app.post("/api/worker/recover")
def recover_worker():
    with stream_connection() as conn:
        conn.execute("""
            UPDATE demo.worker_runtime
            SET desired_state = 'RUNNING', updated_at = NOW()
            WHERE id = 1
        """)
    return {"requested_state": "RUNNING"}


@app.get("/api/worker/status")
def worker_status():
    with stream_connection() as conn:
        state = conn.execute("""
            SELECT desired_state, actual_state, process_pid
            FROM demo.worker_runtime
            WHERE id = 1
        """).fetchone()

        logs = conn.execute("""
            SELECT id, component, message, created_at
            FROM demo.worker_logs
            ORDER BY id DESC
            LIMIT 60
        """).fetchall()

    return {
        "desired_state": state[0],
        "actual_state": state[1],
        "pid": state[2],
        "logs": [
            {
                "id": r[0],
                "component": r[1],
                "message": r[2],
                "timestamp": r[3].isoformat()
            }
            for r in reversed(logs)
        ]
    }
