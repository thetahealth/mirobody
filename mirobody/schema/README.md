# `mirobody/schema` — the database schema

22 SQL files, applied in filename order by `Server.start()` at boot. Four
baselines (`00`–`10`) create the tables; the rest are incremental `ALTER`s.

The three newest are `a5_medications.sql` (medications as an entity, because a
plan has a schedule and a lifecycle and a reading has neither — see
`docs/medications.md`), `a6_observation_model.sql` (every reading, whatever
brought it in, as one append-only fact table with its coding beside it — see
"The observation model" below and `docs/pipeline.md` §6) and
`a7_retire_series_tables.sql` (the tables it replaces, renamed for the
migration).

Gaps in the numbering are deletions, not mistakes — see "Pruning" below.

## Why it lives inside the package

The schema is **code, not documentation**: `mirobody serve` creates its own
tables, so that ability has to travel with the package or
`pip install 'mirobody[app]'` cannot bootstrap anything.

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

**It is a dev convenience, governed by one explicit switch.** The replay runs
while `BOOTSTRAP_SCHEMA` is true (the default — the one-command demo's tables
appear by themselves). A real deployment provisions its schema ahead of time
and sets `BOOTSTRAP_SCHEMA: false` in its overlay; this is not a production
migration path. (An earlier version skipped the replay only for a hardcoded
list of environment names, so a deployment named `ENV=production` — any name
outside the list — got the replay against its provisioned database. Names
carry no behavior anymore.)

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
- **`care_circles`** / **`care_circle_members`**: the care circle, and THE
  authorization throat — `user/care_circle.py::accepted_membership` reads the
  second one to decide whether one person may act on another's record. Each
  member's `health_access` (0 none / 1 read / 2 read-write) says what THEY share
  of THEIR OWN record, defaults to 0, and cannot be raised by anyone else.
  Replaced `th_share_relationship` + `th_share_user_config` +
  `th_share_permission_type` (three tables), which stored a directed grant
  defaulting to `{"all": 1}` — read everything, on by default, chosen by the
  other party. `a3_migrate_share_relationship.sql` carries old rows across.

#### Health Data

- **`health_data_{provider}`**: Raw data storage for specific providers. One table
  per provider that actually ships here — `health_data_garmin`, `health_data_oura`,
  `health_data_whoop` — plus `health_vital_user`.

## Pruning

DDL is dead weight the moment nothing reads or writes it, and here it was also
misleading: it advertised features this project does not have. Removed after
checking every table and column against the code:

| Removed | Why |
| --- | --- |
| `health_data_epic`, `health_data_oracle`, `health_data_libre` | No such providers here (`collect/providers/` ships Garmin, Oura, WHOOP, PostgreSQL). The two Epic/Oracle names survive only as string literals in a `source_table IN (…)` filter. |
| `health_vital_webhook` | No query anywhere touched it. |
| `th_task_flow` | Same; `th_messages.reference_task_id` is read but never written. |
| `th_user_avatar_managed` | Avatars live in `care_circle_members.avatar_key`, which is what the sharing endpoints actually use. |
| `user_agent_prompt`, `user_mcp_config` (`02_`) | Their six `/api/user/{prompt,mcp}/*` endpoints are gone. Nothing in the shipped client could create a saved prompt (the system prompt has one source, `PROMPTS` in config), and nothing — not the client, not the tool loader — ever read a saved MCP server. The MCP feature this project keeps is the other direction: `POST /personal/mcp` mints the URL a person pastes into Claude Desktop. Existing rows are left in place; the baseline stops creating the tables. |
| `th_user_custom_skills` (`31_…`) | Its CRUD router is gone, and nothing ever read this table. |
| `27_add_tags_to_sessions`, `34_add_session_status_fields`, `44_th_sessions_add_status` | `tags`, `read_status`, `write_status`, `ai_status`, `status` — a notes/journal feature that does not exist here. `category` survived (a live `IS NULL` filter) and moved to its owning baseline. |
| `th_messages.comment` + its GIN trigram index | Never read; its only writer was an argument no caller passed. The index paid trigram maintenance on every insert into the busiest table. |
| `th_series_data.full_dim_id` + `idx_th_series_data_full_dim_id` (`42_`) | The second key `42_` added beside `fhir_id`, into `indicator_full_dim` — a dimension table no baseline here creates, owned by a service this project no longer runs. Nothing here read or wrote the column, so the index kept a b-tree over an always-NULL column on every insert into `th_series_data`. `fhir_id` stayed: it is live (`_coding_for`). |

Nothing was dropped from existing databases except four indexes and two
tables (`99_drop_unused_ddl.sql`) — removing DDL from a baseline only changes
what a *new* database gets, and `DROP COLUMN`/`DROP TABLE` would destroy data a
deployment may still hold. Production and staging provision their schema ahead of
time (see "It only runs in dev"), so none of this touches them.

Columns that are inert but harmless were left alone: `th_messages.user_name`,
`group_id`, `updated_at`, `th_sessions.user_name`, and `preview`. Removing one
from a baseline that live databases already ran only buys divergence.

**"Harmless" is about the INDEX and the insert rate, not about the column.**
The obvious phrasing — "a nullable column nobody writes costs no maintenance"
— is the rule this table's own two hardest cases contradict:
`th_series_data.full_dim_id` and `th_messages.comment` were both nullable and
both unwritten, and both had to go, because each carried an index over an
always-NULL column on one of the two busiest tables here. An unindexed column
really is free. An indexed one costs a write per insert, so its price is
whatever that table's insert rate is — which is what made
`idx_th_series_data_full_dim_id` expensive.

## The observation model

`a6_observation_model.sql` is the data layer of ② Translate. One reading is
one row of `th_observation`: the text as printed (`name_text`, `value_text`,
`unit_text`, `ref_text`, never translated or edited), the typed layer derived
from it (`value_kind`, `value_num`, `comparator`, `unit_ucum`), the time with
its zone and the local day computed once at write time, and where it came
from (`modality`, `source_kind`, `source_ref`, `vendor`, the frozen
`th_extraction` it was read out of). The coding sits in `th_coding_current`
(one row per observation: the LOINC code, or `needs-input` / `refused` with a
reason, and the `series_id` either way) with its full history in
`th_coding_history` and the shared reasoning in `th_coding_decision`.

Three rules the tables enforce rather than document:

- **append-only facts.** Nothing UPDATEs `th_observation`. A correction or a
  retraction is a new row pointing at the old one (`amends`); the read view
  `v_observation` hides the old one. The only DELETE is the privacy path
  (`collect/observations.py::erase`), which cascades.
- **one identity.** `(user, name_key, observed_start, observed_end,
  source_ref, source_record_id, member_of, amends)` is unique, so a
  re-uploaded report or a re-sent batch writes nothing twice, and a device
  sync that changed a value amends the row it replaces.
- **the day is decided once.** `th_day_authority` names the observation a
  (person, series, local day) publishes; election writes it, readers join it.

`th_series` is the per-person catalogue an assistant reads first (one row per
series with counts, range and the latest value), refreshed by the writer.
`th_concept` caches the display names and axes of the codes in use.
`th_check_result` holds consistency checks as rows. `th_coding_alias` holds
mappings a person confirmed.

`a7_retire_series_tables.sql` renames `th_series_data`, `th_series_dim`,
`fhir_indicators` and `standard_indicators_device` to `*_retired_15`;
`mirobody migrate-observations` moves the old rows through the new writer.
Drop the retired tables yourself once that has run.

## Applying it by hand

If you need to manually initialize the database (e.g., for production):

1.  Ensure the database exists.
2.  Run the SQL files in order using `psql`, **with `search_path` set**:

```bash
export PGOPTIONS="-c search_path=$PG_SCHEMA"     # theta_ai, unless you changed it
for f in mirobody/schema/*.sql; do
    psql -v ON_ERROR_STOP=1 -h "$PG_HOST" -U "$PG_USER" -d "$PG_DBNAME" -f "$f" || break
done
```

`PGOPTIONS` is the whole point of that first line and it is easy to skip.
**None of these files name a schema**, because `mirobody serve` connects with
`-c search_path=<PG_SCHEMA>` already set (`utils/config/postgresql.py`) and a
hard-coded `theta_ai.` would break any deployment that chose a different one.
Applied by hand without it, every `CREATE TABLE` lands in `public` while the
application keeps reading `theta_ai` — and the failure surfaces far away, as
`relation "th_medication_plan" does not exist` from a tool that looks fine.
(Written down because it happened: `a5_medications.sql` was applied by hand,
landed in `public`, and the medications tool reported an internal error.)

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
