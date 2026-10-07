"""Creating the database schema on a dev server's first run.

Extracted from `Server.start`, which mixed this with binding the socket. The
composition root should register routers, middleware, exception handlers and
lifespan: a 40-line DDL replay is not that, and it is the part most likely to
be read by someone asking "where do my tables come from?".

Two explicit switches govern deployment posture; `ENV` is only an overlay
selector (`config.{ENV}.yaml`) and a log field, and may be any name:

* ``BOOTSTRAP_SCHEMA`` (default true): replay `mirobody/schema/` at boot.
  Real deployments provision the schema ahead of time and set it false; see
  `mirobody/schema/README.md` for the contract those files satisfy.
* ``PRODUCTION`` (default false): declare that this deployment faces real
  users. Refuses to start while demo affordances remain (below), and skips
  the demo seed.

Both are explicit switches rather than something inferred from the
environment NAME, because name-based gating is an allowlist doing a safety
gate's job: `ENV=production`, `ENV=live`, `ENV=prod-eu` (any name an
operator picks that the list did not anticipate) silently gets the DEV
posture: DDL replayed against a provisioned database, demo login codes
accepted, demo data seeded.
"""

from __future__ import annotations

import logging
import os
import secrets

from mirobody.kernel.ops import is_driver_exception

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

    * `EMAIL_PREDEFINE_CODES` non-empty: a predefined code lets anyone who
      knows a listed address sign in (the short-circuit works in the
      production Mandrill/SMTP verifiers too, not just the demo stub). The
      default config ships a code, so a demo box later promoted to production
      would silently keep its anonymous login if only SECURITY.md asked for
      its removal.
    * any config value still reading the `REPLACE_THIS_VALUE_IN_PRODUCTION`
      placeholder: the sentinel's own name says when it must be gone.
    * an empty `JWT_KEY`: the server would come up with no JWT middleware, so
      no request is signed in and the sign-in routes are not rate-limited.

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
        _guard_placeholder_jwt_key(config)
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

    if not config.get_str("JWT_KEY"):
        raise RuntimeError(
            "PRODUCTION is set but JWT_KEY is empty: no request could sign in and the "
            "sign-in routes would not be rate-limited. Set JWT_KEY to a long random "
            "secret (openssl rand -hex 32), then start again."
        )


def _guard_placeholder_jwt_key(config) -> None:
    """A placeholder JWT_KEY is a public signing key: anyone who has read
    config.yaml can mint a token for any account. So this run gets its own key,
    as `mirobody dev` does, and sessions end at restart until JWT_KEY is set.

    Loopback used to keep the placeholder and only warn, but a reverse proxy on
    the same machine puts a loopback server on the internet. On 2026-10-01 a
    token minted with the placeholder for a demo account read that account's
    files from a loopback server. Production refuses to start instead (above)."""
    from mirobody.utils.config.config import PLACEHOLDER_SENTINEL

    if config.get_str("JWT_KEY") not in ("", PLACEHOLDER_SENTINEL):
        return
    os.environ["JWT_KEY"] = secrets.token_hex(32)
    config.refresh()
    logger.warning(
        "JWT_KEY is unset or the shipped placeholder: using a key generated for this run, "
        "so sessions end at restart. Set JWT_KEY to keep them."
    )


async def create_schema(config) -> None:
    """Replay every DDL file in `mirobody/schema/`, in filename order.

    Every file is written to be safely re-runnable, because this replays all of
    them on each start: there is no ledger of what has been applied. Verified by
    running the set three times against a clean database: zero errors.

    A failing file is logged and rolled back on its own; the rest still run. That
    is deliberate: one bad increment should not leave a dev database with half a
    schema and no explanation.
    """
    if not should_bootstrap(config):
        return

    pg_config = config.get_postgresql()

    try:
        conn_ctx = await pg_config.get_async_client()
    except Exception as e:
        # `ensure_postgres_reachable` has just reached Postgres, so this is an
        # outage in the moments since. Outside production the boot goes on
        # without the replay and says so; production fails loudly: a real
        # outage must not become a quiet half-written schema.
        if is_production(config):
            raise
        logger.warning(  # phi: ok a host and a port from config
            f"schema bootstrap skipped: Postgres at {pg_config.host}:{pg_config.port} is unreachable "
            f"({type(e).__name__}). Start it, or set BOOTSTRAP_SCHEMA=false to stop trying."
        )
        return

    async with conn_ctx as conn:
        async with conn.cursor() as cur:
            for schema in pg_config.schema.split(","):
                if schema and isinstance(schema, str) and schema != "public":
                    try:
                        await cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
                        logger.info(f"Schema {schema} has been created.")
                    except Exception as e:
                        logger.error("schema creation failed: error_type=%s", type(e).__name__,
                                     exc_info=not is_driver_exception(e))

            # The DDL ships INSIDE the package: `mirobody serve` creating its own
            # tables is a capability, so it has to travel with the wheel. It is
            # deliberately not under `mirobody/res/`, which LICENSE-3RD-PARTY
            # describes as derived from UMLS/SNOMED/LOINC: our DDL is not.
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
                    logger.error("SQL file failed: file=%s error_type=%s",  # phi: ok a shipped DDL file name
                                 filename, type(e).__name__, exc_info=not is_driver_exception(e))
                    await conn.rollback()

            logger.info("SQL files initialization completed.")



async def realign_dose_slots(config) -> None:
    """Dose events recorded under a slot name their plan no longer projects
    get the name it does (`collect.meds.store.realign_dose_slots`). Gated
    like the schema replay; a failure is logged and the boot goes on, as a
    day that reads as missed is not worth a server that does not start."""
    if not should_bootstrap(config):
        return
    from mirobody.collect.meds.store import realign_dose_slots as realign

    try:
        renamed_count = await realign()
    except Exception as e:
        logger.error("dose slot realignment failed: error_type=%s", type(e).__name__,
                     exc_info=not is_driver_exception(e))
        return
    if renamed_count:
        logger.info("dose events realigned to their plan's slot names: count=%d", renamed_count)


#: Seconds a boot waits for Postgres before it says it cannot reach it.
POSTGRES_CONNECT_TIMEOUT_S = 10


async def ensure_postgres_reachable(config, timeout_s: int = POSTGRES_CONNECT_TIMEOUT_S) -> None:
    """Refuse to start, saying why, when Postgres does not answer in time.

    libpq has no connect timeout by default. The image run on its own
    (`docker run thetahealth4mirobody/mirobody`, no Postgres beside it) waited
    on a TCP connect to config.yaml's placeholder host for as long as the OS
    allowed, logged nothing, and sat at `health: starting` for good."""
    import psycopg

    pg = config.get_postgresql()
    try:
        conn = await psycopg.AsyncConnection.connect(
            host=pg.host, port=pg.port, dbname=pg.database,
            user=pg.user, password=pg.password, connect_timeout=timeout_s,
        )
        await conn.close()
    except psycopg.OperationalError as e:
        # The driver's message can quote the connection string; its type cannot.
        # The address is the `pg` line config.print() wrote just above.
        timeout_seconds = timeout_s
        logger.error(
            "cannot reach Postgres (the pg line above) within %s seconds (%s): Mirobody "
            "runs beside its own Postgres; start both with ./deploy.sh (compose.yaml), "
            "or point PG_HOST / PG_PORT at yours",
            timeout_seconds, type(e).__name__,
        )
        raise RuntimeError(
            f"Cannot reach Postgres at {pg.host}:{pg.port} within {timeout_s} s "
            f"({type(e).__name__}). Start it with ./deploy.sh, or set PG_HOST / PG_PORT."
        ) from None


def demo_sign_in(config) -> dict[str, str] | None:
    """The account to sign in with, for the sign-in page to show, when
    `seed_demo_data` below seeds one; None otherwise.

    The same three conditions as the seed: the flag, not PRODUCTION, and a
    predefined code. The first account is the one the seed makes the circle's
    owner. Only `deploy.sh`'s last line and the README said
    `you@mirobody.ai / 111111`, so a newcomer who missed both met a sign-in form
    with no way in. The codes are public by construction (config.yaml).
    """
    from .demo import enabled

    if not enabled() or is_production(config):
        return None
    codes = config.get_dict("EMAIL_PREDEFINE_CODES", {}) or {}
    for email, code in codes.items():
        if email and code:
            return {"email": str(email), "code": str(code)}
    return None


async def seed_demo_data(config) -> None:
    """Load the care-circle demo fixture when `SEED_DEMO_DATA` says so.

    Gated on the flag alone, NOT on `should_bootstrap()`. The two answer
    different questions: `BOOTSTRAP_SCHEMA` is about who provisions tables,
    while whether you want demo data is a deliberate choice a demo deployment
    makes either way. `compose.yaml` sets it. `PRODUCTION: true` overrides
    both flags' demo-friendliness: synthetic patients never seed there.

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
        logger.error("demo seed failed: error_type=%s", type(e).__name__, exc_info=not is_driver_exception(e))


async def start_schedulers() -> None:
    """Load the device platforms and register the background jobs the server
    runs: the vendor pull, the aggregation of readings and the derived
    indicators. A failure stops the boot."""
    from mirobody.collect import setup_platform_system_async, start_theta_pull_scheduler
    from mirobody.translate import start_aggregate_indicator_scheduler, start_derived_scheduler

    await setup_platform_system_async()
    await start_theta_pull_scheduler()
    await start_aggregate_indicator_scheduler()
    await start_derived_scheduler()
