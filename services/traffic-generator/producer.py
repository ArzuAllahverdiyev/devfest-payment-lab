import json
import os
import time
import uuid
import urllib.request

RATE = float(os.getenv("STREAM_RATE", "2"))
LIMIT = int(os.getenv("STREAM_LIMIT", "10"))
API_URL = os.getenv(
    "PAYMENT_API_URL",
    "http://localhost:8000/payments"
)

if not (0 < RATE <= 5):
    raise ValueError("STREAM_RATE must be between 0 and 5")

if not (1 <= LIMIT <= 100):
    raise ValueError("STREAM_LIMIT must be between 1 and 100")

print(
    f"[STREAM] Starting: {RATE} payments/sec, "
    f"limit={LIMIT}",
    flush=True,
)

next_send = time.monotonic()

for i in range(1, LIMIT + 1):
    payment_id = (
        "PAY-STREAM-"
        + uuid.uuid4().hex[:16].upper()
    )

    payload = {
        "payment_id": payment_id,
        "customer_id": f"CUST-{i:04d}",
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
        result = json.load(response)

        print(
            f"[STREAM] #{i:03d} "
            f"payment={payment_id} "
            f"offset={result['kafka_offset']} "
            f"status={response.status}",
            flush=True,
        )

    next_send += 1 / RATE
    if i < LIMIT:
        time.sleep(max(0, next_send - time.monotonic()))

print(f"[STREAM] Finished: {LIMIT} payments published.")
