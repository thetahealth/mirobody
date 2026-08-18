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
