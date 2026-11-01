import os
import logging
from typing import Literal

import psycopg
from fastapi import FastAPI, HTTPException, Header, Response
from pydantic import BaseModel, ConfigDict, Field

from idempotency import create_idempotent_charge

app = FastAPI(title="DevFest Mock Payment Gateway")
logger = logging.getLogger(__name__)


class ChargeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: str = Field(min_length=1, max_length=64)
    amount_minor: int = Field(strict=True, gt=0)
    currency: Literal["AZN"] = "AZN"


def get_connection():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "paymentlab"),
        user=os.getenv("POSTGRES_USER", "devfest"),
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=5,
    )


@app.get("/health")
def health():
    return {"status": "healthy", "service": "mock-payment-gateway"}


@app.post("/charge", status_code=201)
def create_charge(
    request: ChargeRequest,
    response: Response,
    idempotency_key: str | None = Header(
        default=None, alias="Idempotency-Key"
    ),
):
    if idempotency_key is not None and (
        not idempotency_key.strip() or len(idempotency_key) > 128
    ):
        raise HTTPException(400, "Invalid idempotency key")

    try:
        with get_connection() as conn:

            if idempotency_key:
                charge_id, replayed = create_idempotent_charge(
                    conn,
                    request.payment_id,
                    request.amount_minor,
                    request.currency,
                    idempotency_key,
                )
            else:
                # UNSAFE: DevFest demonstration only.
                charge_id = str(conn.execute(
                    """
                    INSERT INTO gateway.gateway_charges
                        (payment_id, amount_minor, currency, status)
                    VALUES (%s, %s, %s, 'SUCCEEDED')
                    RETURNING charge_id
                    """,
                    (
                        request.payment_id,
                        request.amount_minor,
                        request.currency,
                    ),
                ).fetchone()[0])
                replayed = False

        response.status_code = 200 if replayed else 201

        return {
            "charge_id": charge_id,
            "payment_id": request.payment_id,
            "amount_minor": request.amount_minor,
            "currency": request.currency,
            "status": "SUCCEEDED",
            "idempotency_applied": bool(idempotency_key),
            "replayed": replayed,
        }

    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    except psycopg.Error:
        logger.exception("Gateway database failure")
        raise HTTPException(
            status_code=503,
            detail="Gateway storage unavailable",
        )
