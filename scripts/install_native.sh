#!/usr/bin/env bash
#
# Install Mirobody natively: PostgreSQL with pgvector from the distribution's
# own packages, then the Python package from this checkout. No Docker, and so
# no image pull from Docker Hub.
#
# Two callers, one script. A GitHub Codespace runs it from `postCreateCommand`:
# a codespace that ran Compose would need docker-in-docker, a second network
# stack and a multi-gigabyte pull before the first page, and Zulip, whose stack
# is heavier, provisions its codespaces the same native way. A person on Debian
# or Ubuntu runs it by hand when the registry is not reachable from their
# network.
#
# It installs system packages, so it asks sudo for them, and it writes one
# sudoers rule of its own (see `sudo -u postgres` below). Measured on Ubuntu
# 24.04 (PostgreSQL 16, pgvector 0.6.0), Ubuntu 26.04 (18, 0.8.1) and Debian 13
# (17, 0.8.0); `schema/00_prolog.sql` needs nothing newer than the oldest.

set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v apt-get >/dev/null 2>&1 || ! command -v sudo >/dev/null 2>&1; then
    echo "scripts/install_native.sh needs apt-get and sudo, so Debian or Ubuntu." >&2
    echo "For the Docker install instead, run ./deploy.sh." >&2
    exit 1
fi

DB_USER=mirobody
DB_PASSWORD=mirobody
DB_NAME=mirobody

echo "== Python =="
# 3.12 is the floor (pyproject `requires-python`), which Ubuntu 24.04 meets and
# 22.04 does not: its python3 is 3.10. Checked before the apt work so the
# message arrives before a PostgreSQL nobody can use. `python3-venv` is a
# separate package on Debian and Ubuntu, and a minimal install does not have it.
if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)'; then
    echo "scripts/install_native.sh needs Python 3.12 or newer; $(python3 -V 2>&1) is installed." >&2
    echo "Ubuntu 24.04 and Debian 13 have one. Otherwise use ./deploy.sh." >&2
    exit 1
fi

echo "== PostgreSQL with pgvector =="
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql python3-venv

# The major version is whichever one the `postgresql` metapackage installed:
# 16 on Ubuntu 24.04, 17 on Debian 13, 18 on Ubuntu 26.04, and PGDG's newest
# where its apt repository is configured. A hard-coded 16 stopped the install
# on every current release but Ubuntu 24.04 with `Unable to locate package
# postgresql-16-pgvector`, and taking that package from PGDG put pgvector
# beside a server of another version, which then had no `vector` (#375).
PG_VERSION="$(dpkg-query -W -f='${Depends}' postgresql | grep -o 'postgresql-[0-9]*' | head -1)"
PG_VERSION="${PG_VERSION#postgresql-}"
echo "PostgreSQL ${PG_VERSION}"
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "postgresql-${PG_VERSION}-pgvector"

# Installing a server package creates its `main` cluster, on 5432 unless
# another cluster holds that port already, so the port is read rather than
# assumed, and every `psql` below and the app's `.env` name that cluster.
PG_PORT="$(pg_lsclusters -h | awk -v v="${PG_VERSION}" '$1 == v && $2 == "main" { print $3 }')"
if [[ -z "${PG_PORT}" ]]; then
    echo "scripts/install_native.sh: PostgreSQL ${PG_VERSION} has no cluster named main." >&2
    echo "Create it with: sudo pg_createcluster ${PG_VERSION} main" >&2
    exit 1
fi

# Started by hand rather than through systemd, which a dev container does not
# run. The cluster does not come back on its own after a codespace restart;
# `start_native.sh` starts it again. A start that fails stops the install here,
# with PostgreSQL's own reason, instead of at the first `psql`.
sudo pg_ctlcluster "${PG_VERSION}" main status >/dev/null 2>&1 \
    || sudo pg_ctlcluster "${PG_VERSION}" main start

# `sudo -u postgres` is outside the codespace image's `(root)` grant, so it
# falls through to a password prompt, and where no terminal can answer one it
# waits forever: measured at over ten minutes before it was killed. The rule is
# written only when `-n` cannot already reach the account, and every call below
# is `-n` so a missing rule fails instead of hanging.
if ! sudo -n -u postgres true 2>/dev/null; then
    echo "== sudo rule for the postgres account =="
    printf '%s ALL=(postgres) NOPASSWD: ALL\n' "$(id -un)" \
        | sudo tee /etc/sudoers.d/postgres-nopasswd >/dev/null
    sudo chmod 0440 /etc/sudoers.d/postgres-nopasswd
    sudo visudo -c >/dev/null
fi

echo "== database =="
# `CREATE EXTENSION` needs a superuser, and the Compose stack's POSTGRES_USER is
# one too, so this role matches it.
sudo -n -u postgres psql --cluster "${PG_VERSION}/main" -tAc \
        "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" | grep -q 1 \
    || sudo -n -u postgres psql --cluster "${PG_VERSION}/main" -q -c \
        "CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}' SUPERUSER"
sudo -n -u postgres psql --cluster "${PG_VERSION}/main" -tAc \
        "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1 \
    || sudo -n -u postgres createdb --cluster "${PG_VERSION}/main" -O "${DB_USER}" "${DB_NAME}"

echo "== data bundles =="
# Without this the resolver reads LFS pointer stubs and every lookup raises.
git lfs install --local
git lfs pull

echo "== Python package =="
# A virtualenv, as docs/quickstart.md documents it. Ubuntu's system Python is
# externally managed (PEP 668), and the console script has to be on PATH for
# `start_native.sh`, which is simpler to arrange than to explain.
python3 -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -e '.[app]'

# `.env` is gitignored and `Config.init` reads it first, which is where
# `deploy.sh` keeps its settings too. The keys are written once: regenerating
# them would sign every session out and leave the stored provider credentials
# and encrypted record fields undecryptable. Without PG_ENCRYPTION_KEY (1.5.7
# to 1.5.9) those fields used config.yaml's public placeholder (SECURITY.md).
if [[ ! -f .env ]]; then
    echo "== .env =="
    umask 077
    {
        # `mirobody dev` takes a URL and nothing else: `--pg-url`, `PG_URL` or
        # `DATABASE_URL`. `serve` and the worker read the `PG_*` keys instead,
        # so both spellings are written.
        echo "PG_URL=postgres://${DB_USER}:${DB_PASSWORD}@127.0.0.1:${PG_PORT}/${DB_NAME}"
        echo "PG_HOST=127.0.0.1"
        echo "PG_PORT=${PG_PORT}"
        echo "PG_USER=${DB_USER}"
        echo "PG_PASSWORD=${DB_PASSWORD}"
        echo "PG_DBNAME=${DB_NAME}"
        echo "PG_SCHEMA=public"
        echo "HTTP_HOST=0.0.0.0"
        echo "HTTP_PORT=18060"
        echo "SEED_DEMO_DATA=true"
        echo "JWT_KEY=$(.venv/bin/python -c 'import secrets; print(secrets.token_hex(32))')"
        echo "CONFIG_ENCRYPTION_KEY=$(.venv/bin/python -c 'import secrets; print(secrets.token_hex(16))')"
        echo "PG_ENCRYPTION_KEY=$(.venv/bin/python -c 'import secrets; print(secrets.token_hex(32))')"
        # Pinned so the link `start_native.sh` prints keeps working across a
        # restart. Unset, the first-run page mints a new token per process and
        # any link printed earlier is dead.
        echo "SETUP_TOKEN=$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(18))')"
    } > .env
    # The umask alone is not enough in a codespace: `/workspaces` carries a
    # default ACL, which takes precedence over it. Measured, `.env` came out
    # 666 and the postgres account could read every key in it.
    chmod 600 .env
fi

echo
echo "Installed. Start it with scripts/start_native.sh"
