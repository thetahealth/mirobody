#!/usr/bin/env bash
#
# Back up the two volumes that hold irreplaceable data: the Postgres database
# (health readings, care circles, LangGraph conversation checkpoints) and the
# uploaded files, when they live locally rather than in a bucket.
#
# Backup only, on purpose. Restore is documented in docs/backup-restore.md and
# stays a human decision: a script that runs psql against a live database is a
# footgun, and the one time you need it you want to be reading the steps.
#
# Usage:
#   shell/backup.sh                       # -> ./backups
#   BACKUP_DIR=/mnt/nas/mirobody shell/backup.sh
#   0 3 * * *  cd /srv/mirobody && BACKUP_DIR=/mnt/nas/mirobody shell/backup.sh >> /var/log/mirobody-backup.log 2>&1
#
set -euo pipefail
# The dump and the archive are health records: readable by this user only.
umask 077

cd "$(dirname "${BASH_SOURCE[0]}")/.."

BACKUP_DIR="${BACKUP_DIR:-./backups}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
PG_USER="${PG_USER:-holistic_user}"
env_setting() {
    [[ -f .env ]] || return 0
    sed -n "s/^${1}=//p" .env | head -n 1
}
PG_DB="${PG_DB:-${PG_DBNAME:-$(env_setting PG_DBNAME)}}"
PG_DB="${PG_DB:-holistic_db}"

# Compose prefixes every volume with the project name, so the volume declared
# as `mirobody_upload` is really `<project>_mirobody_upload` on the daemon.
# Getting this wrong does not fail: `docker run -v mirobody_upload:/data`
# CREATES a new empty volume and tars nothing, which is why every volume below
# is checked to exist before it is read.
PROJECT="${COMPOSE_PROJECT_NAME:-$(env_setting COMPOSE_PROJECT_NAME)}"
PROJECT="${PROJECT:-$(basename "$PWD")}"
# Compose lowercases the name and keeps only [a-z0-9_-].
PROJECT="$(printf '%s' "$PROJECT" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]//g')"

STAMP="$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP_DIR"

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*"; }

have_volume() { docker volume inspect "$1" >/dev/null 2>&1; }

#-----------------------------------------------------------------------------
# 1. Postgres — logical dump, not a tar of the data directory.
#
# `docker run -v <pgdata>:/data:ro alpine tar` on a RUNNING server copies a
# torn cluster: the files move under the tar. pg_dump is consistent by
# construction (it runs in one transaction) and restores into any pgvector
# image, not only a byte-identical one.
#-----------------------------------------------------------------------------
if ! docker compose ps --services --status running 2>/dev/null | grep -qx pg; then
    log "FATAL: compose service 'pg' is not running — start the stack, or dump from wherever your database actually runs."
    exit 1
fi

DUMP="$BACKUP_DIR/mirobody-db-$STAMP.dump"
STAGED="/tmp/mirobody-db-$STAMP.dump"
log "dumping database $PG_DB (custom format) -> $DUMP"

# Dumped and verified INSIDE the container, then copied out — deliberately not
# `pg_dump ... > file` on the host. Two reasons, both learned by running it:
# a custom-format archive is not seekable through a pipe, so the verification
# below cannot read one from stdin ("did not find magic string in file header"
# on a file whose header is fine); and a dump that dies half-way through a
# redirect leaves a plausible-looking truncated file behind.
#
# A backup nobody has read is a hope, not a backup: pg_restore --list parses
# the archive's table of contents, so it fails on a truncated or empty file.
docker compose exec -T pg sh -c \
    "pg_dump -U '$PG_USER' -d '$PG_DB' -Fc -f '$STAGED' && pg_restore --list '$STAGED' > /dev/null"
# Streamed out rather than `docker compose cp`: that writes through the
# daemon, which on snap Docker sees its own private /tmp and refused a
# BACKUP_DIR under the host's.
if ! docker compose exec -T pg cat "$STAGED" > "$DUMP.partial"; then
    rm -f "$DUMP.partial"
    docker compose exec -T pg rm -f "$STAGED"
    log "FATAL: could not copy the dump out of the pg container"
    exit 1
fi
docker compose exec -T pg rm -f "$STAGED"
mv "$DUMP.partial" "$DUMP"
log "dump verified readable ($(du -h "$DUMP" | cut -f1))"

#-----------------------------------------------------------------------------
# 2. Uploaded files — only when they are on this host.
#
# storage/factory.py tries each cloud backend (Aliyun OSS, AWS S3) first and
# falls back to local, so a deployment with either configured keeps its files
# in a bucket and this volume is empty or absent. Back up the bucket instead;
# this script says so rather than writing a 45-byte tar that looks like success.
#-----------------------------------------------------------------------------
UPLOAD_VOLUME="${PROJECT}_mirobody_upload"
if have_volume "$UPLOAD_VOLUME"; then
    FILES="$BACKUP_DIR/mirobody-uploads-$STAMP.tar.gz"
    log "archiving $UPLOAD_VOLUME -> $FILES"
    # A Docker VM may map a host path such as /tmp to its own /tmp.
    # Stream the archive to a host-side file and verify it before renaming.
    PARTIAL="${FILES}.partial"
    if ! docker run --rm -v "$UPLOAD_VOLUME":/data:ro \
        alpine tar czf - -C /data . > "$PARTIAL"; then
        rm -f "$PARTIAL"
        log "FATAL: upload archive failed"
        exit 1
    fi
    if ! tar tzf "$PARTIAL" >/dev/null; then
        rm -f "$PARTIAL"
        log "FATAL: upload archive could not be read"
        exit 1
    fi
    mv "$PARTIAL" "$FILES"
    log "uploads archived ($(du -h "$FILES" | cut -f1))"
else
    log "skip: no volume $UPLOAD_VOLUME. Either this deployment stores files in a bucket (back up the bucket), or the stack has never written an upload."
fi

#-----------------------------------------------------------------------------
# 3. Retention — pattern-scoped, so a BACKUP_DIR that holds anything else
#    keeps it.
#-----------------------------------------------------------------------------
if [ "$RETENTION_DAYS" -gt 0 ]; then
    log "pruning backups older than $RETENTION_DAYS days"
    find "$BACKUP_DIR" -maxdepth 1 -type f \
        \( -name 'mirobody-db-*.dump' -o -name 'mirobody-uploads-*.tar.gz' \) \
        -mtime "+$RETENTION_DAYS" -print -delete
fi

log "done. Restore steps: docs/backup-restore.md"
