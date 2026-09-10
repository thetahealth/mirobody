# Backing up and restoring

Health data is irreplaceable — a reading you lose is a blood draw you cannot
repeat. This page is the answer to "what do I copy, and how do I get it back".

Every command here was run against a live stack while writing it, including the
restore: the dump below was restored into a scratch database and came back with
29 tables in the `theta_ai` schema and the `vector` extension in place.

## What actually holds your data

`compose.yaml` declares four volumes. They are not equally precious, and
treating them alike is how people back up a pip cache and miss their uploads.

| Declared in compose | Holds | Back it up? |
| --- | --- | --- |
| `mirobody_postgres` | Everything the app knows: readings, files metadata, care circles, users — plus LangGraph's `checkpoint_*` conversation memory when `AGENT_CHECKPOINTER: true`. All inside the `PG_SCHEMA` schema (`theta_ai`), with `vector(1024)` columns and an hnsw index. | **Yes — `pg_dump`.** Not a tar of the data directory (see below). |
| `mirobody_upload` | The original uploaded PDFs and photos — **only when files are stored locally.** | **Yes, if it exists.** `storage/factory.py` tries each cloud backend (Aliyun OSS, AWS S3) first and falls back to local, so a deployment with either configured keeps its files in a bucket: back up the bucket, and this volume will be absent or empty. |
| `mirobody_redis` | The worker's task queues (`LPUSH`/`BLPOP` lists, no TTL) and the distributed locks — which is why compose runs redis with `--maxmemory-policy noeviction --appendonly yes`. | **No, by default.** This is in-flight work, not history. Restoring a stale AOF re-runs tasks that already ran, or silently drops ones that had not. `INCLUDE_REDIS=1` captures it for forensics. |
| `mirobody_site_packages` | A pip cache, keyed on a hash of `pyproject.toml` + `requirements.txt`. | **Never.** It is derived, and restoring it reinstates dependencies the current code no longer imports — the release that dropped a 245 MB vendor SDK would get it back. Delete the volume and the next start reinstalls. |

There is **no `mirobody_charts` volume**. The `ChartService` MCP tools that
rendered PNGs through a Node toolchain were removed; the agent now writes a
fenced ```vis-chart``` data block that the frontend renders, so nothing writes
chart files at all.

### Two things that make copy-pasted commands fail silently

**Volume names are project-prefixed.** The volume declared as
`mirobody_upload` is `<project>_mirobody_upload` on the daemon — for a checkout
in `mirobody/`, that is `mirobody_mirobody_upload`. `docker volume ls` shows
the real names. This matters because getting it wrong does not error:

```bash
# WRONG — creates a NEW empty volume and tars nothing. Exit code 0.
docker run --rm -v mirobody_upload:/data:ro -v "$PWD":/b alpine tar czf /b/x.tar.gz /data
```

`shell/backup.sh` checks `docker volume inspect` before reading anything, so a
name that does not exist is reported rather than archived as 45 empty bytes.

**Container names come from service names.** There is no `container_name:` in
`compose.yaml` and the database service is `pg`, so the container is
`mirobody-pg-1`, not `mirobody-db-1`. Prefer `docker compose exec pg …`, which
does not care what the container is called.

## Backing up

```bash
shell/backup.sh                                  # -> ./backups
BACKUP_DIR=/mnt/nas/mirobody shell/backup.sh     # somewhere that is not this disk
RETENTION_DAYS=30 INCLUDE_REDIS=1 shell/backup.sh
```

Nightly, via cron:

```cron
0 3 * * *  cd /srv/mirobody && BACKUP_DIR=/mnt/nas/mirobody shell/backup.sh >> /var/log/mirobody-backup.log 2>&1
```

What it does, and why:

- **`pg_dump -Fc`, not a tar of `/var/lib/postgresql/data`.** Tarring a running
  cluster copies files that are moving under the tar; the result restores, or
  does not, depending on timing. `pg_dump` is consistent by construction and
  restores into any pgvector image rather than only a byte-identical one.
- **The dump is verified before the run is called a success.** `pg_restore
  --list` parses the archive's table of contents, which fails on a truncated or
  empty file. A backup nobody has read is a hope, not a backup.
- **The dump is written and verified inside the container, then copied out.** A
  custom-format archive is not seekable through a pipe, so verification cannot
  read one from stdin, and `pg_dump > file` that dies half-way leaves a
  plausible-looking truncated file.
- **Uploads are archived only if the local volume exists** (see the table).
- **Retention is pattern-scoped** — it prunes `mirobody-db-*.dump`,
  `mirobody-uploads-*.tar.gz` and `mirobody-redis-*.tar.gz` older than
  `RETENTION_DAYS`, and leaves anything else in the directory alone.

The script is backup-only on purpose. Restore stays a human decision: the one
time you need it, you want to be reading the steps.

## Restoring

Stop the app first so nothing writes while the schema is being replaced. Keep
`pg` running — it is what does the restoring.

```bash
docker compose stop mirobody mirobody_worker
```

### Database

Into a **fresh** database (the safe path — the old one stays until you are
satisfied):

```bash
docker compose cp backups/mirobody-db-20260902-153442.dump pg:/tmp/restore.dump
docker compose exec -T pg createdb -U holistic_user holistic_db_restored
docker compose exec -T pg pg_restore -U holistic_user -d holistic_db_restored --no-owner /tmp/restore.dump
# sanity-check before switching over
docker compose exec -T pg psql -U holistic_user -d holistic_db_restored -c \
  "select count(*) from information_schema.tables where table_schema='theta_ai'"
```

Then point `PG_DBNAME` at `holistic_db_restored` in your config and start the
app. To restore over the existing database instead, add `--clean` (it drops
each object before recreating it) and be aware that there is no undo:

```bash
docker compose exec -T pg pg_restore -U holistic_user -d holistic_db --clean --no-owner /tmp/restore.dump
```

A note on the vector columns: they restore as ordinary data, and the pinned
`pgvector/pgvector` image already has the extension, so **no re-embedding is
needed** after a restore. (Re-embedding is only for changing
`UTILS_EMBEDDING_MODEL` or the `model` of the `MODELS` entry carrying that `embedding:` family.)

### Uploaded files

```bash
docker run --rm \
  -v mirobody_mirobody_upload:/data \
  -v "$PWD/backups":/backup \
  alpine tar xzf /backup/mirobody-uploads-20260902-153442.tar.gz -C /data
```

Check the real volume name with `docker volume ls` first. The archive is made
with `-C /data .`, so it extracts as the volume's contents, not as a nested
`data/` directory.

### Then

```bash
docker compose up -d
```

## Before upgrading

1. **Run `shell/backup.sh` and keep the output off this machine.** This is the
   whole checklist, because it is also the rollback: there are no down
   migrations.
2. Read `CHANGELOG.md` for the version you are moving to.
3. `git pull && ./deploy.sh` — the script takes no arguments and already does
   `compose down`, then `up -d --remove-orphans`, then tails the logs.
4. Watch that log tail: the schema DDL runs on the first start (see below), and
   a file that fails is logged with its `sql_filename`.

### How the schema actually changes

`server/bootstrap.py::create_schema` replays **every** file in
`mirobody/schema/*.sql`, in filename order, on each start. Every file is
written to be safely re-runnable — there is no ledger of what has been applied,
and no schema-version table by design. Verified by running the whole set three
times against a clean database: zero errors. A failing file is rolled back on
its own and logged; the rest still run, so one bad increment cannot leave half
a schema with no explanation.

Two consequences worth knowing:

- **Upgrades are additive.** A new release adds tables or columns on first
  start; you do not run a migration command.
- **There is no automated downgrade.** Rolling back a release that added a
  column means restoring the dump you took in step 1. That is why step 1 is
  step 1.
