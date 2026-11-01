from datetime import datetime, timezone
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


class PaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: str = Field(min_length=1, max_length=64)
    customer_id: str = Field(min_length=1, max_length=64)
    amount_minor: int = Field(strict=True, gt=0)
    currency: Literal["AZN"] = "AZN"


class PaymentRequestedEvent(PaymentRequest):
    schema_version: Literal[1] = 1
    event_type: Literal["PaymentRequested"] = "PaymentRequested"

    event_id: str = Field(
        default_factory=lambda: str(uuid4())
    )

    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
