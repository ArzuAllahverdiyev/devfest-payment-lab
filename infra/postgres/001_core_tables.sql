
-- ==========================================
-- DEVFEST PAYMENT LAB
-- Migration 001: Core Database Tables
-- ==========================================

BEGIN;

-- 1. PAYMENTS
CREATE TABLE IF NOT EXISTS app.payments (
    payment_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency VARCHAR(3) NOT NULL DEFAULT 'AZN'
        CHECK (currency = 'AZN'),

    status TEXT NOT NULL DEFAULT 'CREATED'
        CHECK (status IN (
            'CREATED', 'PENDING', 'PROCESSING',
            'SUCCEEDED', 'FAILED', 'UNKNOWN'
        )),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. CONSUMER INBOX
CREATE TABLE IF NOT EXISTS app.consumer_inbox (
    consumer_group TEXT NOT NULL,
    event_id TEXT NOT NULL,
    payment_id TEXT NOT NULL
        REFERENCES app.payments(payment_id),

    status TEXT NOT NULL DEFAULT 'PROCESSING'
        CHECK (status IN ('PROCESSING', 'COMPLETED')),

    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    attempts INTEGER NOT NULL DEFAULT 0
        CHECK (attempts >= 0),

    lease_owner TEXT,
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,

    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (consumer_group, event_id)
);

-- 3. GATEWAY CHARGES
CREATE TABLE IF NOT EXISTS gateway.gateway_charges (
    charge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    payment_id TEXT NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency VARCHAR(3) NOT NULL
        CHECK (currency = 'AZN'),

    status TEXT NOT NULL DEFAULT 'SUCCEEDED'
        CHECK (status IN ('SUCCEEDED', 'FAILED')),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. GATEWAY IDEMPOTENCY
CREATE TABLE IF NOT EXISTS gateway.gateway_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    request_fingerprint TEXT NOT NULL,

    charge_id UUID NOT NULL UNIQUE
        REFERENCES gateway.gateway_charges(charge_id),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- INDEXES
CREATE INDEX IF NOT EXISTS idx_payments_status
    ON app.payments(status);

CREATE INDEX IF NOT EXISTS idx_inbox_recovery
    ON app.consumer_inbox(status, lease_expires_at);

CREATE INDEX IF NOT EXISTS idx_charges_payment
    ON gateway.gateway_charges(payment_id);

COMMIT;

