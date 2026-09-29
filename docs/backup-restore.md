# Backing up and restoring

Health data is irreplaceable — a reading you lose is a blood draw you cannot
repeat. This page is the answer to "what do I copy, and how do I get it back".

Every command here was run against a live stack while writing it, including the
restore: the dump below was restored into a scratch database with the
`theta_ai` schema and `vector` extension in place.

## What actually holds your data

`compose.yaml` declares two volumes. The database and uploaded files both need
backups; the application image can be pulled again.

| Declared in compose | Holds | Back it up? |
| --- | --- | --- |
| `mirobody_postgres` | Everything the app knows: readings, files metadata, care circles, users, the `th_task_queue` background tasks, short-lived authentication state in `th_ephemeral`, and LangGraph's `checkpoint_*` conversation memory when `AGENT_CHECKPOINTER: true`. All inside the `PG_SCHEMA` schema (`theta_ai`), with `vector(1024)` columns and an hnsw index. | **Yes — `pg_dump`.** Not a tar of the data directory (see below). A restored task queue can replay work that finished after the dump. |
| `mirobody_upload` | The original uploaded PDFs and photos — **only when files are stored locally.** | **Yes, if it exists.** `storage/factory.py` tries each cloud backend (Aliyun OSS, AWS S3) first and falls back to local, so a deployment with either configured keeps its files in a bucket: back up the bucket, and this volume will be absent or empty. |

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
- **The upload archive streams into a host file and is read back before it is
  accepted.** A Docker VM can map a host `/tmp` bind mount to its own
  `/tmp`, leaving a successful tar command but no archive on the host.
- **Retention is pattern-scoped** — it prunes `mirobody-db-*.dump`,
  `mirobody-uploads-*.tar.gz` older than
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
# Authentication challenges and rate-limit windows are not safe to replay.
docker compose exec -T pg psql -U holistic_user -d holistic_db_restored -c \
  'TRUNCATE theta_ai.th_ephemeral'
# sanity-check before switching over
docker compose exec -T pg psql -U holistic_user -d holistic_db_restored -c \
  "select count(*) from information_schema.tables where table_schema='theta_ai'"
```

Then set `PG_DBNAME=holistic_db_restored` in `.env` and start the
app. To restore over the existing database instead, add `--clean` (it drops
each object before recreating it) and be aware that there is no undo:

```bash
docker compose exec -T pg pg_restore -U holistic_user -d holistic_db --clean --no-owner /tmp/restore.dump
docker compose exec -T pg psql -U holistic_user -d holistic_db -c \
  'TRUNCATE theta_ai.th_ephemeral'
```

A note on the vector columns: they restore as ordinary data, and the pinned
`pgvector/pgvector` image already has the extension. Nothing writes new
vectors any more (the semantic tier went in 1.5.0), so a restore needs no
re-embedding.

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
3. `git pull && ./deploy.sh` — the script keeps existing volumes and pulls the
   selected application image before waiting for healthy services.
4. Watch that log tail: the schema DDL runs on the first start (see below), and
   a file that fails is logged with its `sql_filename`.

### Upgrading from 1.5.2

1.5.3 runs a built image with Postgres as its only state service; 1.5.2 ran
the checkout with Redis beside it. `./deploy.sh` does the rest, with four
things to know first:

- **Stop the worker and let it finish** (`docker compose stop mirobody_worker`
  on the 1.5.2 checkout) if a profile refresh was queued. Redis's queue is not
  carried over; a lost refresh is redone at the next upload.
- **A `compose.override.yaml` written for 1.5.2 stops the deploy.** It names
  the redis service and mounts the checkout over `/app`. Move it aside and
  start from the current example:

  ```bash
  mv compose.override.yaml compose.override.yaml.1.5.2
  cp compose.override.yaml.example compose.override.yaml
  ```

- **Secrets carry over.** The overlay's `JWT_KEY` stays in charge, so nobody
  is signed out. A database the 1.5.2 compose stack created keeps its
  password and its field-encryption key, which were `config.yaml`'s
  placeholder (see SECURITY.md before exposing it). A value the app encrypted
  in the overlay that compose needs in plain text (`PG_PASSWORD`) is asked for.
- **Afterwards** the redis container is removed (`--remove-orphans`); the
  script names the two 1.5.2 volumes that are no longer used
  (`*_mirobody_redis`, `*_mirobody_site_packages`) for `docker volume rm`.
  Personal MCP links from 1.5.2 stop working: make new ones in Settings. The
  first start builds one index on `th_observation` (`idx_th_observation_user_created`),
  which on a large table holds writes to it for that long.

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
