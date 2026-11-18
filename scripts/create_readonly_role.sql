-- Create a dedicated read-only PostgreSQL role for bi-chart-mcp.
--
-- Run as a database owner or superuser, for example:
--   psql "$ADMIN_DSN" \
--     -v role=bi_reader -v password='change-me' -v database=analytics -v schema=public \
--     -f scripts/create_readonly_role.sql
--
-- Repeat the GRANT block for every schema listed in the source's `schemas`.
-- The role must NOT be a member of any pg_* predefined role (pg_read_server_files,
-- pg_execute_server_program, pg_signal_backend, ...), and must not own any objects.

CREATE ROLE :"role" LOGIN PASSWORD :'password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    CONNECTION LIMIT 8;

-- Defaults for every session of this role; bi-chart-mcp also sets them per transaction.
ALTER ROLE :"role" SET default_transaction_read_only = on;
ALTER ROLE :"role" SET statement_timeout = '30s';
ALTER ROLE :"role" SET lock_timeout = '2s';
ALTER ROLE :"role" SET idle_in_transaction_session_timeout = '60s';

GRANT CONNECT ON DATABASE :"database" TO :"role";

GRANT USAGE ON SCHEMA :"schema" TO :"role";
GRANT SELECT ON ALL TABLES IN SCHEMA :"schema" TO :"role";
-- Tables created later in this schema (by the role running this script) stay readable.
ALTER DEFAULT PRIVILEGES IN SCHEMA :"schema" GRANT SELECT ON TABLES TO :"role";
