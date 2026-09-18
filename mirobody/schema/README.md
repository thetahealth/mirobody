# `mirobody/schema`: the database schema

One file per functional domain, applied in filename order by `Server.start()`
at boot:

| File | Domain |
| --- | --- |
| `00_prolog.sql` | extensions; the encryption functions every other file relies on |
| `10_accounts.sql` | the account, passkeys, the generated profile, care circles (and the one-way migration from the tables they replaced) |
| `20_files.sql` | uploaded documents |
| `30_observations.sql` | every reading as one row of `th_observation`, its coding beside it; genotypes |
| `31_medications.sql` | medications as an entity: plan, course, dose event, override |
| `40_devices.sql` | provider connections, raw payloads, the point buffer, source priority |
| `41_device_rules.sql` | the value range a device reading must fall in, with its seed |
| `50_chat.sql` | sessions, messages, share links |
| `90_retire.sql` | what a database built from older files still carries: renames and drops |

Prefixes are two digits and stay two digits: the replay sorts names as
strings, and `100_` would run before `30_`.

## The contract

**Every statement is re-runnable.** The bootstrap replays all files on every
start and keeps no ledger, so: `IF NOT EXISTS` on `CREATE TABLE`, `CREATE
INDEX` and `ADD COLUMN`; `OR REPLACE` on functions and views; `ON CONFLICT` on
seed inserts; `to_regclass` guards around anything else. A file that fails is
rolled back on its own and logged; the rest still run.

**A CREATE carries the full current column list, and an ALTER follows for
every column that arrived later.** The CREATE is what a fresh database gets.
The `ADD COLUMN IF NOT EXISTS` beneath it is the only thing that upgrades a
database created before the column existed. Both live in the domain file, next
to the table.

**A retirement goes in `90_retire.sql`, not in a delete.** Removing a statement
from a domain file only changes what a new database gets. A table with a
person's history is renamed (`*_retired_15`), never dropped here; a table or
index that held none is dropped.

**It is a dev convenience.** The replay runs while `BOOTSTRAP_SCHEMA` is true
(the default). A real deployment provisions its schema ahead of time and sets
`BOOTSTRAP_SCHEMA: false`; this is not a production migration path.

**Nothing names a schema.** `mirobody serve` connects with
`-c search_path=<PG_SCHEMA>` set (`utils/config/postgresql.py`). pgcrypto lives
in `public`, so the search_path lists both.

## Why it lives inside the package

`mirobody serve` creates its own tables, so the DDL travels with the wheel. It
is not under `mirobody/res/`: that directory is licensed terminology data
(`LICENSE-3RD-PARTY`), and our own DDL is not.

## The observation model

`30_observations.sql` is the data layer of ② Translate. One reading is one row
of `th_observation`: the text as printed (`name_text`, `value_text`,
`unit_text`, `ref_text`, never translated or edited), the typed layer derived
from it (`value_kind`, `value_num`, `comparator`, `unit_ucum`), the time with
its zone and the local day computed once at write time, and where it came from
(`modality`, `source_kind`, `source_ref`, `vendor`, the frozen `th_extraction`
it was read out of). The coding sits in `th_coding_current` (the LOINC code, or
`needs-input` / `refused` with a reason, and the `series_id` either way) with
its history in `th_coding_history` and the shared reasoning in
`th_coding_decision`.

Three rules the tables enforce rather than document:

- **append-only facts.** Nothing UPDATEs `th_observation`. A correction or a
  retraction is a new row pointing at the old one (`amends`); the view
  `v_observation` hides the old one. The only DELETE is the privacy path
  (`collect/observations.py::erase`), which cascades.
- **one identity.** `(user, name_key, observed_start, observed_end,
  source_ref, source_record_id, member_of, amends)` is unique, so a re-uploaded
  report or a re-sent batch writes nothing twice.
- **the day is decided once.** `th_day_authority` names the observation a
  (person, series, local day) publishes; election writes it, readers join it.

`th_series` is the per-person catalogue an assistant reads first. `th_concept`
caches display names and axes of the codes in use. `th_check_result` holds
consistency checks as rows. `th_coding_alias` holds mappings a person confirmed.

`90_retire.sql` renames `th_series_data`, `th_series_dim`, `fhir_indicators`
and `standard_indicators_device` to `*_retired_15`; `mirobody
migrate-observations` moves the old rows through the new writer. Drop the
retired tables yourself once that has run.

## Applying it by hand

```bash
export PGOPTIONS="-c search_path=$PG_SCHEMA,public"     # theta_ai, unless you changed it
for f in mirobody/schema/*.sql; do
    psql -v ON_ERROR_STOP=1 -h "$PG_HOST" -U "$PG_USER" -d "$PG_DBNAME" -f "$f" || break
done
```

Without `PGOPTIONS` every table lands in `public` while the application reads
`theta_ai`, and the failure surfaces far away as `relation ... does not exist`.

## Changing it

Edit the domain file that owns the table. A new column goes into the CREATE
and gets its `ADD COLUMN IF NOT EXISTS` beneath it. A new table goes into the
file of its domain, or a new two-digit file when it opens one. Anything that
removes or renames goes into `90_retire.sql`. Do not add a numbered increment
file: that is the shape this directory had before, and it took twenty-two
files to say what nine say now.

`tests/schema_fingerprint.py` (local) applies a directory to a scratch
database and prints its catalogue; the reorganization was checked by diffing
the old chain against the new one, twice-applied, and the old chain followed
by the new one.

## Why not Alembic

It fits technically (raw SQL migrations, a revision table, package resources)
and it solves a problem this repo does not have yet: applying each change
exactly once to a database that holds data you cannot recreate. Today the
bootstrap runs only in dev, where the answer to a bad migration is "drop the
database". The trigger to adopt it is the first production schema change that
must be applied to live data.
