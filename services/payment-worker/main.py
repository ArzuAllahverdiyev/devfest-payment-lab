import os
import time

import httpx
import psycopg
from confluent_kafka import Consumer, KafkaException

from shared.payment_schema import PaymentRequestedEvent
from inbox import claim_event, complete_event


GROUP_ID = "devfest-payment-worker"


def db_connection():
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        autocommit=True,
    )



def demo_checkpoint_after_charge(conn, event, lease_token):
    """Wait for presenter command after a successful gateway charge."""

    with conn.transaction():
        row = conn.execute(
            """
            SELECT action, phase
            FROM demo.experiment_controls
            WHERE payment_id = %s
            FOR UPDATE
            """,
            (event.payment_id,),
        ).fetchone()

        # Normal payments keep their existing behavior.
        if row is None:
            return

        action, phase = row

        # After a manual crash, recovery must not pause again.
        if phase == "CRASHED":
            conn.execute(
                """
                UPDATE demo.experiment_controls
                SET phase = 'RECOVERING', updated_at = NOW()
                WHERE payment_id = %s
                """,
                (event.payment_id,),
            )
            conn.execute(
                """
                INSERT INTO demo.experiment_events
                    (payment_id, component, event_type, message)
                VALUES (%s, 'WORKER', 'RECOVERY',
                        'Redelivered payment; skipping pause')
                """,
                (event.payment_id,),
            )
            print("[DEMO] Recovery detected; bypassing pause", flush=True)
            return

        if phase in ("RECOVERING", "RECOVERED", "COMPLETED"):
            return

        if action == "CONTINUE":
            return

        conn.execute(
            """
            UPDATE demo.experiment_controls
            SET phase = 'PAUSED_AFTER_CHARGE', updated_at = NOW()
            WHERE payment_id = %s
            """,
            (event.payment_id,),
        )
        conn.execute(
            """
            INSERT INTO demo.experiment_events
                (payment_id, component, event_type, message)
            VALUES (%s, 'WORKER', 'PAUSED_AFTER_CHARGE',
                    'Charge committed; waiting for presenter command')
            """,
            (event.payment_id,),
        )

    print("[DEMO] PAUSED_AFTER_CHARGE", flush=True)

    while True:
        with conn.transaction():
            renewed = conn.execute(
                """
                UPDATE app.consumer_inbox
                SET lease_expires_at = NOW() + INTERVAL '30 seconds'
                WHERE consumer_group = %s
                  AND event_id::text = %s
                  AND lease_token::text = %s
                  AND status = 'PROCESSING'
                  AND lease_expires_at > NOW()
                RETURNING event_id
                """,
                (
                    GROUP_ID,
                    str(event.event_id),
                    str(lease_token),
                ),
            ).fetchone()

            if renewed is None:
                raise RuntimeError(
                    "Inbox lease expired or ownership lost during pause"
                )

            command = conn.execute(
                """
                SELECT action
                FROM demo.experiment_controls
                WHERE payment_id = %s
                FOR UPDATE
                """,
                (event.payment_id,),
            ).fetchone()

            if command is None:
                raise RuntimeError("Experiment control disappeared")

            action = command[0]

            if action in ("CRASH", "CONTINUE"):
                phase = "CRASHED" if action == "CRASH" else "CONTINUING"

                conn.execute(
                    """
                    UPDATE demo.experiment_controls
                    SET phase = %s, updated_at = NOW()
                    WHERE payment_id = %s
                    """,
                    (phase, event.payment_id),
                )
                conn.execute(
                    """
                    INSERT INTO demo.experiment_events
                        (payment_id, component, event_type, message)
                    VALUES (%s, 'WORKER', %s, %s)
                    """,
                    (
                        event.payment_id,
                        action,
                        "Presenter selected " + action,
                    ),
                )

        if action == "CRASH":
            print("[DEMO] Manual crash requested!", flush=True)
            os._exit(77)

        if action == "CONTINUE":
            print("[DEMO] Presenter selected CONTINUE", flush=True)
            return

        time.sleep(2)


def process_payment(msg):
    event = PaymentRequestedEvent.model_validate_json(msg.value())

    print(f"[KAFKA] Received {event.payment_id}, offset={msg.offset()}", flush=True)

    with db_connection() as conn:

        # Claim the event or wait for an existing lease to expire.
        for _ in range(25):
            action, token = claim_event(
                conn,
                event,
                GROUP_ID,
                msg.topic(),
                msg.partition(),
                msg.offset(),
                owner=f"worker-{os.getpid()}",
                lease_seconds=15,
            )

            if action != "BUSY":
                break

            print("[INBOX] Active lease; waiting...", flush=True)
            time.sleep(1)
        else:
            raise TimeoutError("Could not acquire inbox lease")

        print(f"[INBOX] {action}", flush=True)

        if action == "CLAIMED":
            payload = {
                "payment_id": event.payment_id,
                "amount_minor": event.amount_minor,
                "currency": event.currency,
            }

            headers = {"Idempotency-Key": event.payment_id}

            # Fault-injection option for our isolated demo.
            if os.getenv("DEMO_DISABLE_IDEMPOTENCY") == "1":
                headers = {}

            response = httpx.post(
                os.environ["GATEWAY_URL"] + "/charge",
                json=payload,
                headers=headers,
                timeout=5,
            )
            response.raise_for_status()

            charge = response.json()

            if (
                charge["status"] != "SUCCEEDED"
                or charge["payment_id"] != event.payment_id
                or charge["amount_minor"] != event.amount_minor
                or charge["currency"] != event.currency
            ):
                raise RuntimeError("Gateway returned inconsistent charge")

            print(
                f"[GATEWAY] Charge={charge['charge_id']}, "
                f"replayed={charge['replayed']}",
                flush=True,
            )

            # Real process crash AFTER gateway commit,
            # BEFORE inbox completion and Kafka offset commit.
            if os.getenv("DEMO_CRASH_AFTER_CHARGE") == "1":
                print("[CRASH] Killing consumer now!", flush=True)
                os._exit(77)

            demo_checkpoint_after_charge(conn, event, token)

            complete_event(conn, event, GROUP_ID, token)

            print("[INBOX] COMPLETED", flush=True)

    # Only after durable processing.
    return True


def main():
    consumer = Consumer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
        ),
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "max.poll.interval.ms": 1800000,
    })

    consumer.subscribe(["payment.requests"])
    print("[WORKER] Started", flush=True)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            process_payment(msg)

            committed = consumer.commit(
                message=msg,
                asynchronous=False,
            )

            if not committed or any(p.error is not None for p in committed):
                raise RuntimeError("Kafka offset commit failed")

            print(
                f"[KAFKA] Offset {msg.offset() + 1} committed",
                flush=True,
            )

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
