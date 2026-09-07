"""Creating the database schema on a dev server's first run.

Extracted from `Server.start`, which mixed this with binding the socket. The
composition root should register routers, middleware, exception handlers and
lifespan — a 40-line DDL replay is not that, and it is the part most likely to
be read by someone asking "where do my tables come from?".

Two explicit switches govern deployment posture; `ENV` is only an overlay
selector (`config.{ENV}.yaml`) and a log field, and may be any name:

* ``BOOTSTRAP_SCHEMA`` (default true) — replay `mirobody/schema/` at boot.
  Real deployments provision the schema ahead of time and set it false; see
  `mirobody/schema/README.md` for the contract those files satisfy.
* ``PRODUCTION`` (default false) — declare that this deployment faces real
  users. Refuses to start while demo affordances remain (below), and skips
  the demo seed.

Both are explicit switches rather than something inferred from the
environment NAME, because name-based gating is an allowlist doing a safety
gate's job: `ENV=production`, `ENV=live`, `ENV=prod-eu` — any name an
operator picks that the list did not anticipate — silently gets the DEV
posture: DDL replayed against a provisioned database, demo login codes
accepted, demo data seeded.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_SCHEMA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "schema")


def should_bootstrap(config) -> bool:
    """True when this deployment wants the boot-time DDL replay."""
    return config.get_bool("BOOTSTRAP_SCHEMA", True)


def is_production(config) -> bool:
    """True when the operator declared `PRODUCTION: true` (config or env)."""
    return config.get_bool("PRODUCTION", False)


def enforce_production_auth_safety(config) -> None:
    """Refuse to start a production deployment that still looks like the demo.

    `PRODUCTION: true` is the operator's declaration; this is what it buys.
    Two refusals, both for things that are exactly right in the one-command
    local demo and exactly wrong on the public internet:

    * `EMAIL_PREDEFINE_CODES` non-empty — a predefined code lets anyone who
      knows a listed address sign in (the short-circuit works in the
      production Mandrill/SMTP verifiers too, not just the demo stub). The
      default config ships a code, so a demo box later promoted to production
      would silently keep its anonymous login if only SECURITY.md asked for
      its removal.
    * any config value still reading the `REPLACE_THIS_VALUE_IN_PRODUCTION`
      placeholder — the sentinel's own name says when it must be gone.

    Failing the boot makes the operator fix these deliberately instead of us
    guessing which of them were intentional.
    """
    if not is_production(config):
        # The one nudge that survives the name-list removal: an ENV named
        # like production with the switch unset is almost certainly the old
        # convention, and everything this guard prevents would sail through.
        env_name = (os.environ.get("ENV") or "").strip()
        if "prod" in env_name.lower():
            logger.warning(
                "ENV=%s looks like a production deployment, but PRODUCTION is "
                "not set — demo login codes and placeholder secrets are NOT "
                "being rejected. Environment names carry no behavior; set "
                "PRODUCTION: true (see SECURITY.md).", env_name,
            )
        return

    codes = config.get_dict("EMAIL_PREDEFINE_CODES", {}) or {}
    if codes:
        raise RuntimeError(
            f"PRODUCTION is set but EMAIL_PREDEFINE_CODES contains "
            f"{len(codes)} entr{'y' if len(codes) == 1 else 'ies'} "
            f"({', '.join(sorted(codes))}). Predefined codes are a login bypass — anyone "
            "who knows a listed address can sign in with its hardcoded code. Remove "
            "EMAIL_PREDEFINE_CODES from the production config (see SECURITY.md, "
            "'Before you expose this to a network'), then start again."
        )

    placeholders = config.placeholder_keys()
    if placeholders:
        raise RuntimeError(
            f"PRODUCTION is set but {len(placeholders)} config value"
            f"{' is' if len(placeholders) == 1 else 's are'} still the shipped "
            f"placeholder ({', '.join(placeholders)}). Replace each "
            "REPLACE_THIS_VALUE_IN_PRODUCTION with a real secret, then start "
            "again."
        )


async def create_schema(config) -> None:
    """Replay every DDL file in `mirobody/schema/`, in filename order.

    Every file is written to be safely re-runnable, because this replays all of
    them on each start — there is no ledger of what has been applied. Verified by
    running the set three times against a clean database: zero errors.

    A failing file is logged and rolled back on its own; the rest still run. That
    is deliberate: one bad increment should not leave a dev database with half a
    schema and no explanation.
    """
    if not should_bootstrap(config):
        return

    pg_config = config.get_postgresql()

    async with await pg_config.get_async_client(cursor_factory=None) as conn:
        async with conn.cursor() as cur:
            for schema in pg_config.schema.split(","):
                if schema and isinstance(schema, str) and schema != "public":
                    try:
                        await cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                        logger.info(f"Schema {schema} has been created.")
                    except Exception as e:
                        logger.error(str(e), exc_info=True)

            # The DDL ships INSIDE the package: `mirobody serve` creating its own
            # tables is a capability, so it has to travel with the wheel. It is
            # deliberately not under `mirobody/res/`, which LICENSE-3RD-PARTY
            # describes as derived from UMLS/SNOMED/LOINC — our DDL is not.
            if not os.path.isdir(_SCHEMA_DIR):
                logger.warning(
                    "schema bootstrap skipped: %s is missing. Provision the schema "
                    "yourself, or reinstall the package.", _SCHEMA_DIR,
                )
                return

            for filename in sorted(os.listdir(_SCHEMA_DIR)):
                if not filename.endswith(".sql"):
                    continue
                with open(os.path.join(_SCHEMA_DIR, filename), encoding="utf-8") as f:
                    statements = f.read()
                try:
                    await cur.execute(statements)
                    await conn.commit()
                    logger.info(f"SQL file {filename} executed successfully.")
                except Exception as e:
                    logger.error(str(e), exc_info=True, extra={"sql_filename": filename})
                    await conn.rollback()

            logger.info("SQL files initialization completed.")

    # The DDL adds the day columns; this fills them on the rows that predate
    # them, so a day-grained read never has to fall back to padding a naive
    # timestamp a day each way. Idempotent and bounded — see pulse/backfill.py.
    try:
        from ..pulse.backfill import backfill_day_columns
        await backfill_day_columns()
    except Exception as e:
        # A history that is not backfilled still reads correctly, with
        # `window_semantics="date_padded_naive"`. Never a boot failure.
        logger.warning("day-column backfill skipped: error_type=%s", type(e).__name__)

async def seed_demo_data(config) -> None:
    """Load the care-circle demo fixture when `SEED_DEMO_DATA` says so.

    Gated on the flag alone, NOT on `should_bootstrap()`. The two answer
    different questions: `BOOTSTRAP_SCHEMA` is about who provisions tables,
    while whether you want demo data is a deliberate choice a demo deployment
    makes either way. `compose.yaml` sets it. `PRODUCTION: true` overrides
    both flags' demo-friendliness — synthetic patients never seed there.

    Failure is logged and swallowed. A demo that cannot load is a disappointing
    first run; a server that will not boot because of one is worse.
    """
    from .demo import enabled, seed

    if not enabled():
        return

    if is_production(config):
        # Synthetic patients do not belong in a store that also holds real
        # ones. (With EMAIL_PREDEFINE_CODES already rejected at boot under
        # PRODUCTION, the seed's care circle would have no owner anyway.)
        logger.warning(
            "SEED_DEMO_DATA is set but PRODUCTION is set too — skipping demo seed."
        )
        return

    members = list((config.get_dict("EMAIL_PREDEFINE_CODES", {}) or {}).keys())
    if not members:
        logger.warning(
            "SEED_DEMO_DATA is set but EMAIL_PREDEFINE_CODES is empty — seeded data "
            "would belong to a care circle nobody can sign in to. Skipping."
        )
        return

    try:
        await seed(members)
    except Exception as e:
        logger.error("demo seed failed: %s", e, exc_info=True)
