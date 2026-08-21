"""Creating the database schema on a dev server's first run.

Extracted from `Server.start`, which mixed this with binding the socket. The
composition root should register routers, middleware, exception handlers and
lifespan — a 40-line DDL replay is not that, and it is the part most likely to
be read by someone asking "where do my tables come from?".

Nothing here runs in a real deployment: `TEST`, `GRAY`, `PROD` and
`TEST-INLOCAL` all use a schema provisioned ahead of time. See
`mirobody/schema/README.md` for the contract those files must satisfy, and for
why Alembic is not adopted yet.
"""

from __future__ import annotations

import logging
import os

SKIP_ENVIRONMENTS = ("TEST", "GRAY", "PROD", "TEST-INLOCAL")

_SCHEMA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "schema")


def should_bootstrap() -> bool:
    """True only for local/dev environments."""
    return (os.environ.get("ENV") or "").strip().upper() not in SKIP_ENVIRONMENTS


async def create_schema(config) -> None:
    """Replay every DDL file in `mirobody/schema/`, in filename order.

    Every file is written to be safely re-runnable, because this replays all of
    them on each start — there is no ledger of what has been applied. Verified by
    running the set three times against a clean database: zero errors.

    A failing file is logged and rolled back on its own; the rest still run. That
    is deliberate: one bad increment should not leave a dev database with half a
    schema and no explanation.
    """
    if not should_bootstrap():
        return

    pg_config = config.get_postgresql()

    async with await pg_config.get_async_client(cursor_factory=None) as conn:
        async with conn.cursor() as cur:
            for schema in pg_config.schema.split(","):
                if schema and isinstance(schema, str) and schema != "public":
                    try:
                        await cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                        logging.info(f"Schema {schema} has been created.")
                    except Exception as e:
                        logging.error(str(e), exc_info=True)

            # The DDL ships INSIDE the package: `mirobody serve` creating its own
            # tables is a capability, so it has to travel with the wheel. It is
            # deliberately not under `mirobody/res/`, which LICENSE-3RD-PARTY
            # describes as derived from UMLS/SNOMED/LOINC — our DDL is not.
            if not os.path.isdir(_SCHEMA_DIR):
                logging.warning(
                    "schema bootstrap skipped: %s is missing. Provision the schema "
                    "yourself, or reinstall the package.", _SCHEMA_DIR,
                )
                return

            for filename in sorted(os.listdir(_SCHEMA_DIR)):
                if not filename.endswith(".sql"):
                    continue
                with open(os.path.join(_SCHEMA_DIR, filename), "r", encoding="utf-8") as f:
                    statements = f.read()
                try:
                    await cur.execute(statements)
                    await conn.commit()
                    logging.info(f"SQL file {filename} executed successfully.")
                except Exception as e:
                    logging.error(str(e), exc_info=True, extra={"sql_filename": filename})
                    await conn.rollback()

            logging.info("SQL files initialization completed.")

async def seed_demo_data(config) -> None:
    """Load the care-circle demo fixture when `SEED_DEMO_DATA` says so.

    Gated on the flag alone, NOT on `should_bootstrap()`. The two answer
    different questions: `create_schema` must never touch a provisioned
    deployment, while whether you want demo data is a deliberate choice a
    demo deployment makes regardless of its ENV. `compose.yaml` sets it.

    Failure is logged and swallowed. A demo that cannot load is a disappointing
    first run; a server that will not boot because of one is worse.
    """
    from ..demo import enabled, seed

    if not enabled():
        return

    members = list((config.get_dict("EMAIL_PREDEFINE_CODES", {}) or {}).keys())
    if not members:
        logging.warning(
            "SEED_DEMO_DATA is set but EMAIL_PREDEFINE_CODES is empty — seeded data "
            "would belong to a care circle nobody can sign in to. Skipping."
        )
        return

    try:
        await seed(members)
    except Exception as e:
        logging.error("demo seed failed: %s", e, exc_info=True)
