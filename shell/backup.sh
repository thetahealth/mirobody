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
#   RETENTION_DAYS=30 INCLUDE_REDIS=1 shell/backup.sh
#   0 3 * * *  cd /srv/mirobody && BACKUP_DIR=/mnt/nas/mirobody shell/backup.sh >> /var/log/mirobody-backup.log 2>&1
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

BACKUP_DIR="${BACKUP_DIR:-./backups}"
RETENTION_DAYS="${RETENTION_DAYS:-7}"
INCLUDE_REDIS="${INCLUDE_REDIS:-0}"
PG_USER="${PG_USER:-holistic_user}"
PG_DB="${PG_DB:-holistic_db}"

# Compose prefixes every volume with the project name, so the volume declared
# as `mirobody_upload` is really `<project>_mirobody_upload` on the daemon.
# Getting this wrong does not fail: `docker run -v mirobody_upload:/data`
# CREATES a new empty volume and tars nothing, which is why every volume below
# is checked to exist before it is read.
PROJECT="${COMPOSE_PROJECT_NAME:-$(basename "$PWD")}"

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
docker compose cp "pg:$STAGED" "$DUMP"
docker compose exec -T pg rm -f "$STAGED"
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
    docker run --rm \
        -v "$UPLOAD_VOLUME":/data:ro \
        -v "$(cd "$BACKUP_DIR" && pwd)":/backup \
        alpine tar czf "/backup/$(basename "$FILES")" -C /data .
    log "uploads archived ($(du -h "$FILES" | cut -f1))"
else
    log "skip: no volume $UPLOAD_VOLUME. Either this deployment stores files in a bucket (back up the bucket), or the stack has never written an upload."
fi

#-----------------------------------------------------------------------------
# 3. Redis — off by default.
#
# It holds the worker's task queues (LPUSH/BLPOP lists, no TTL) and the
# distributed locks, which is why compose runs it with `noeviction` and
# `appendonly yes`. That is IN-FLIGHT work, not history: restoring a stale AOF
# re-runs tasks that already ran, or drops ones that did not. INCLUDE_REDIS=1
# captures it anyway for forensics.
#-----------------------------------------------------------------------------
if [ "$INCLUDE_REDIS" = "1" ]; then
    REDIS_VOLUME="${PROJECT}_mirobody_redis"
    if have_volume "$REDIS_VOLUME"; then
        AOF="$BACKUP_DIR/mirobody-redis-$STAMP.tar.gz"
        log "archiving $REDIS_VOLUME -> $AOF (forensics only — see docs/backup-restore.md)"
        docker compose exec -T redis redis-cli -a "${REDIS_PASSWORD:-}" --no-auth-warning BGSAVE >/dev/null 2>&1 || true
        docker run --rm \
            -v "$REDIS_VOLUME":/data:ro \
            -v "$(cd "$BACKUP_DIR" && pwd)":/backup \
            alpine tar czf "/backup/$(basename "$AOF")" -C /data .
    else
        log "skip: no volume $REDIS_VOLUME"
    fi
fi

# The site-packages volume is deliberately never here: it is a pip cache keyed
# on a hash of pyproject.toml + requirements.txt, and restoring it reinstates
# dependencies the current code no longer imports (1.4.0 dropped a 245 MB SDK
# that a restored volume would put straight back).

#-----------------------------------------------------------------------------
# 4. Retention — pattern-scoped, so a BACKUP_DIR that holds anything else
#    keeps it.
#-----------------------------------------------------------------------------
if [ "$RETENTION_DAYS" -gt 0 ]; then
    log "pruning backups older than $RETENTION_DAYS days"
    find "$BACKUP_DIR" -maxdepth 1 -type f \
        \( -name 'mirobody-db-*.dump' -o -name 'mirobody-uploads-*.tar.gz' -o -name 'mirobody-redis-*.tar.gz' \) \
        -mtime "+$RETENTION_DAYS" -print -delete
fi

log "done. Restore steps: docs/backup-restore.md"
