-- Load-test table schemas for the dsql-replicator demo.
--
-- These are the tables the load generator targets (`generate_load.py --tables
-- orders,payments,invoices,shipments,refunds`). They must exist on BOTH the
-- source and target DSQL clusters BEFORE any CDC traffic flows; the replicator
-- does not create or alter base schemas on its own (DDL is not replicated —
-- see docs/OVERVIEW.md §9).
--
-- To apply from a shell with ada credentials:
--   PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token \
--     --hostname <HOST> --region <REGION> --endpoint-url https://dsql.<REGION>.api.aws) \
--   psql "host=<HOST> port=5432 dbname=postgres user=admin sslmode=require" \
--     -f loadtest/schemas.sql

CREATE TABLE IF NOT EXISTS orders (
    id         uuid        NOT NULL,
    bucket     integer     NOT NULL,
    payload    text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS payments (
    id         uuid        NOT NULL,
    bucket     integer     NOT NULL,
    payload    text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS invoices (
    id         uuid        NOT NULL,
    bucket     integer     NOT NULL,
    payload    text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS shipments (
    id         uuid        NOT NULL,
    bucket     integer     NOT NULL,
    payload    text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS refunds (
    id         uuid        NOT NULL,
    bucket     integer     NOT NULL,
    payload    text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);
