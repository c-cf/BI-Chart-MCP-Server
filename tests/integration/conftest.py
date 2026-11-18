"""PostgreSQL fixtures: a throwaway container, seeded data and a read-only role.

Requires a running Docker daemon. Run with `uv run pytest -m integration`.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import psycopg
import pytest

from support.postgres import (
    DATABASE,
    READER_PASSWORD,
    READER_ROLE,
    SEED,
    PostgresTarget,
    render_role_script,
    with_credentials,
)


@pytest.fixture(scope="session")
def postgres() -> Iterator[PostgresTarget]:
    # Ryuk (the reaper sidecar) cannot get a port mapping on some Docker Desktop setups;
    # the context manager below removes the container anyway.
    os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer("postgres:17-alpine", dbname=DATABASE, driver=None) as container:
        admin_dsn = container.get_connection_url()
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(SEED)
            conn.execute(render_role_script())  # type: ignore[arg-type]
        yield PostgresTarget(
            admin_dsn=admin_dsn,
            reader_dsn=with_credentials(admin_dsn, READER_ROLE, READER_PASSWORD),
        )
