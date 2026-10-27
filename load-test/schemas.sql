-- Load-test table schemas for the dsql-replicator demo.
--
-- These are the two tables the load generator targets (`generate_load.py --tables
-- orders,payments`). They must exist on BOTH the source and target DSQL clusters
-- BEFORE any CDC traffic flows; the replicator does not create or alter base
-- schemas on its own (DDL is not replicated — see docs/OVERVIEW.md §9).
--
-- Captured 2026-10-26 from the two live clusters we were running against:
--   source: <SOURCE_CLUSTER_ID>.dsql.us-east-1.on.aws
--   target: <TARGET_CLUSTER_ID>.dsql.us-east-2.on.aws
-- Both clusters had identical shape; this file is the authoritative snapshot.
--
-- To apply from a shell with ada credentials:
--   PGPASSWORD=$(aws dsql generate-db-connect-admin-auth-token \
--     --hostname <HOST> --region <REGION> --endpoint-url https://dsql.<REGION>.api.aws) \
--   psql "host=<HOST> port=5432 dbname=postgres user=admin sslmode=require" \
--     -f load-test/schemas.sql

CREATE TABLE IF NOT EXISTS orders (
    id         uuid                     NOT NULL,
    bucket     integer                  NOT NULL,
    payload    text,
    updated_at timestamptz              NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS payments (
    id         uuid                     NOT NULL,
    bucket     integer                  NOT NULL,
    payload    text,
    updated_at timestamptz              NOT NULL DEFAULT now(),
    PRIMARY KEY (id)
);
