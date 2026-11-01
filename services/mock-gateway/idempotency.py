import hashlib
import json


def request_fingerprint(payment_id, amount_minor, currency):
    payload = {
        "payment_id": payment_id,
        "amount_minor": amount_minor,
        "currency": currency,
    }

    raw = json.dumps(payload, sort_keys=True).encode()
    return hashlib.sha256(raw).hexdigest()


def create_idempotent_charge(
    conn, payment_id, amount_minor, currency, key
):
    fingerprint = request_fingerprint(
        payment_id, amount_minor, currency
    )

    # Serialize concurrent requests for the same key.
    conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
        ("gateway:" + key,),
    )

    existing = conn.execute(
        """
        SELECT i.request_fingerprint, c.charge_id
        FROM gateway.gateway_idempotency i
        JOIN gateway.gateway_charges c
          ON c.charge_id = i.charge_id
        WHERE i.idempotency_key = %s
        """,
        (key,),
    ).fetchone()

    if existing:
        old_fingerprint, charge_id = existing

        if old_fingerprint != fingerprint:
            raise ValueError(
                "Idempotency key reused with different payment data"
            )

        return str(charge_id), True

    charge_id = conn.execute(
        """
        INSERT INTO gateway.gateway_charges
            (payment_id, amount_minor, currency, status)
        VALUES (%s, %s, %s, 'SUCCEEDED')
        RETURNING charge_id
        """,
        (payment_id, amount_minor, currency),
    ).fetchone()[0]

    conn.execute(
        """
        INSERT INTO gateway.gateway_idempotency
            (idempotency_key, request_fingerprint, charge_id)
        VALUES (%s, %s, %s)
        """,
        (key, fingerprint, charge_id),
    )

    return str(charge_id), False
