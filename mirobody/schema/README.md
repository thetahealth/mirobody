# `mirobody/schema` — the database schema

26 SQL files, applied in filename order by `Server.start()` at boot. Four
baselines (`00`–`10`) create the tables; the rest are incremental `ALTER`s.

Gaps in the numbering are deletions, not mistakes — see "Pruning" below.

## Why it lives inside the package

The schema is **code, not documentation**: `mirobody serve` creates its own
tables, so that ability has to travel with the package or
`pip install 'mirobody[agents]'` cannot bootstrap anything.

It deliberately does **not** sit under `mirobody/res/`. That directory is the
licensed terminology data, and `LICENSE-3RD-PARTY` describes everything in it as
derived from UMLS/SNOMED/LOINC — which our own DDL is not. Keeping the two apart
is what makes that statement true.

## The contract

**Every file must be safely re-runnable.** The bootstrap replays *all* of them
on every start; there is no ledger of what has already been applied. In practice
that means `IF NOT EXISTS` on `CREATE TABLE`/`CREATE INDEX`/`ADD COLUMN`, `OR
REPLACE` on functions, and `ON CONFLICT DO NOTHING` on seed inserts. Verified by
running the whole set three times against a clean database: zero errors.

**It only runs in dev.** `Server.start()` skips the bootstrap when `ENV` is
`TEST`, `GRAY`, `PROD` or `TEST-INLOCAL` — those deployments use a schema
provisioned ahead of time. So this is a convenience for local work, not a
production migration path.

## What the schema contains

All tables reside in the `theta_ai` schema.

### Extensions
The following PostgreSQL extensions are enabled:
- **`vector`**: For AI embeddings and semantic search.
- **`pg_trgm`**: For fast text similarity search.
- **`pgcrypto`**: For cryptographic functions.

### Core Tables

#### User & Auth
- **`health_app_user`**: Main user profile table.
- **`health_user_provider`**: Stores connection info for external providers (Google, Apple, etc.).

#### Data Sharing
- **`th_share_relationship`**: Tracks who shares data with whom.
- **`th_share_permission_type`**: Defines granular permissions (e.g., "All Data", "Device Data").

#### Health Data

- **`health_data_{provider}`**: Raw data storage for specific providers. One table
  per provider that actually ships here — `health_data_garmin`, `health_data_oura`,
  `health_data_whoop` — plus `health_vital_user`.

#### Agent Workspace

- **`deep_agent_workspace`**: PostgreSQL-backed storage for DeepAgent's virtual
  filesystem (`ls`, `read_file`, `write_file`, `edit_file`, `glob`, `grep` — the
  native deepagents FilesystemMiddleware tools, not MCP tools).

One row per file, keyed `(user_id, session_id, scope, path)`, where `scope`
isolates the mounts (`workspace` / `memory` / `uploads` / `library`) that a single
`CompositeBackend` layers over this one table. Large or binary payloads are
offloaded to object storage; `content` keeps extracted text so `grep` still works.

**Implementation:** `mirobody/agent/deep/backend.py` (`PgFilesystemBackend`) | Schema: `mirobody/schema/90_deepagents.sql`

## Pruning

DDL is dead weight the moment nothing reads or writes it, and here it was also
misleading: it advertised features this project does not have. Removed after
checking every table and column against the code:

| Removed | Why |
| --- | --- |
| `health_data_epic`, `health_data_oracle`, `health_data_libre` | No such providers here (`pulse/providers/` ships Garmin, Oura, WHOOP, PostgreSQL). The two Epic/Oracle names survive only as string literals in a `source_table IN (…)` filter. |
| `health_vital_webhook` | No query anywhere touched it. |
| `th_task_flow` | Same; `th_messages.reference_task_id` is read but never written. |
| `th_user_avatar_managed` | Avatars live in `th_share_user_config.avatar_key`, which is what the sharing endpoints actually use. |
| `th_user_custom_skills` (`31_…`) | Its CRUD router is gone: DeepAgent loads Agent Skills from `SKILL_DIRS` on disk, so nothing ever read this table. |
| `27_add_tags_to_sessions`, `34_add_session_status_fields`, `44_th_sessions_add_status` | `tags`, `read_status`, `write_status`, `ai_status`, `status` — a notes/journal feature that does not exist here. `category` survived (a live `IS NULL` filter) and moved to its owning baseline. |
| `th_messages.comment` + its GIN trigram index | Never read; its only writer was an argument no caller passed. The index paid trigram maintenance on every insert into the busiest table. |

Nothing was dropped from existing databases except those two indexes
(`99_drop_unused_chat_indexes.sql`) — removing DDL from a baseline only changes
what a *new* database gets, and `DROP COLUMN`/`DROP TABLE` would destroy data a
deployment may still hold. Production and staging provision their schema ahead of
time (see "It only runs in dev"), so none of this touches them.

Columns that are inert but harmless were left alone: `th_messages.user_name`,
`group_id`, `updated_at`, `th_sessions.user_name`, `preview`, and
`deep_agent_workspace.metadata`. A nullable column nobody writes costs no
maintenance; removing it from a baseline that live databases already ran only
buys divergence.

## Applying it by hand

If you need to manually initialize the database (e.g., for production):

1.  Ensure the database exists.
2.  Run the SQL files in order using `psql`:

```bash
psql -h $PG_HOST -U $PG_USER -d $PG_DBNAME -f mirobody/schema/00_init_schema.sql
psql -h $PG_HOST -U $PG_USER -d $PG_DBNAME -f mirobody/schema/01_basedata.sql
# ... run remaining files
```

## Adding a change

Add a new file with the next free numeric prefix. Do not renumber existing ones:
the order is the only thing that makes the chain deterministic.

**Do not fold your change back into a baseline file as well.** That is how the
two duplicates found in this repo happened — `th_sessions.category` was added by
both `01_basedata.sql` and the since-deleted `27_add_tags_to_sessions.sql`, and
`common_part_encrypted` by both `00_init_schema.sql` and `96_…`. Both were
harmless (`IF NOT EXISTS`), which is exactly why they survived: nothing failed,
but it stopped being clear which file owns a column. Pick one owner.

## Why not Alembic

It is the obvious candidate and it does fit technically: Alembic does not require
ORM models (`op.execute()` takes raw SQL), it tracks applied revisions in an
`alembic_version` table, and migrations can ship as package resources. This
project has none of the usual blockers — no ORM models exist here at all, so
nothing would need rewriting.

The reason to wait is that Alembic solves a problem this repo does not have yet.
Its value is applying each change exactly once, in order, to a database that
holds data you cannot recreate. Today the bootstrap runs only in dev, where the
answer to a bad migration is "drop the database", and production schemas are
provisioned separately.

**The trigger to adopt it** is the first time a production deployment needs a
schema change applied to live data. At that point the "replay everything, hope
it is idempotent" model stops being adequate — it cannot express a backfill that
must run once, or a rename, or a downgrade. Do it then, not before.

Sources: [Alembic tutorial](https://alembic.sqlalchemy.org/en/latest/tutorial.html)
