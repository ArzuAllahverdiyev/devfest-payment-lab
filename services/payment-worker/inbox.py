from uuid import uuid4


def claim_event(
    conn, event, group, topic, partition,
    offset, owner, lease_seconds=30
):
    token = uuid4()

    with conn.transaction():

        conn.execute("""
            INSERT INTO app.payments
                (payment_id, customer_id, amount_minor,
                 currency, status)
            VALUES (%s, %s, %s, %s, 'PROCESSING')
            ON CONFLICT (payment_id) DO NOTHING
        """, (
            event.payment_id,
            event.customer_id,
            event.amount_minor,
            event.currency,
        ))

        payment = conn.execute("""
            SELECT customer_id, amount_minor, currency
            FROM app.payments
            WHERE payment_id = %s
            FOR UPDATE
        """, (event.payment_id,)).fetchone()

        if payment != (
            event.customer_id,
            event.amount_minor,
            event.currency,
        ):
            raise ValueError(
                "Payment ID reused with different payment data"
            )

        created = conn.execute("""
            INSERT INTO app.consumer_inbox (
                consumer_group, event_id, payment_id,
                kafka_topic, kafka_partition, kafka_offset,
                status, attempts, lease_owner,
                lease_token, lease_expires_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s,
                'PROCESSING', 1, %s, %s,
                NOW() + make_interval(secs => %s)
            )
            ON CONFLICT (consumer_group, event_id)
            DO NOTHING
            RETURNING event_id
        """, (
            group, event.event_id, event.payment_id,
            topic, partition, offset,
            owner, token, lease_seconds,
        )).fetchone()

        if created:
            return "CLAIMED", token

        existing = conn.execute("""
            SELECT payment_id, status,
                   lease_expires_at > NOW()
            FROM app.consumer_inbox
            WHERE consumer_group = %s AND event_id = %s
            FOR UPDATE
        """, (group, event.event_id)).fetchone()

        if existing[0] != event.payment_id:
            raise ValueError("Event ID belongs to another payment")

        if existing[1] == "COMPLETED":
            return "COMPLETED", None

        if existing[2]:
            return "BUSY", None

        conn.execute("""
            UPDATE app.consumer_inbox
            SET lease_owner = %s,
                lease_token = %s,
                lease_expires_at =
                    NOW() + make_interval(secs => %s),
                attempts = attempts + 1,
                updated_at = NOW()
            WHERE consumer_group = %s AND event_id = %s
        """, (
            owner, token, lease_seconds,
            group, event.event_id,
        ))

        return "CLAIMED", token


def complete_event(conn, event, group, lease_token):
    """Atomically complete payment and inbox records."""

    with conn.transaction():
        result = conn.execute("""
            UPDATE app.consumer_inbox
            SET status = 'COMPLETED',
                completed_at = NOW(),
                updated_at = NOW(),
                lease_owner = NULL,
                lease_token = NULL,
                lease_expires_at = NULL
            WHERE consumer_group = %s
              AND event_id = %s
              AND payment_id = %s
              AND status = 'PROCESSING'
              AND lease_token = %s
              AND lease_expires_at > clock_timestamp()
            RETURNING event_id
        """, (
            group,
            event.event_id,
            event.payment_id,
            lease_token,
        )).fetchone()

        if result is None:
            raise RuntimeError(
                "Inbox completion rejected: lease expired "
                "or ownership lost"
            )

        payment = conn.execute("""
            UPDATE app.payments
            SET status = 'SUCCEEDED',
                updated_at = NOW()
            WHERE payment_id = %s
            RETURNING payment_id
        """, (event.payment_id,)).fetchone()

        if payment is None:
            raise RuntimeError("Payment record not found")
