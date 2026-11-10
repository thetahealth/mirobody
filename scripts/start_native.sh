#!/usr/bin/env bash
#
# Start Postgres, the task worker and the HTTP server.
#
# Both callers of `install_native.sh` use this. In a codespace it runs from
# `postStartCommand` and `postAttachCommand`, both of which fire repeatedly, so
# it exits as soon as the server already answers; `postStartCommand` is what
# brings the stack back when a stopped codespace resumes. On a machine, run it
# by hand, in its own terminal or under `nohup`.
#
# The server creates the schema and seeds the demo record on first start
# (`server/bootstrap.py`), so there is no separate migrate or seed step.

set -euo pipefail
cd "$(dirname "$0")/.."

PG_VERSION=16
LOG_DIR="${TMPDIR:-/tmp}"

if [[ ! -x .venv/bin/mirobody ]]; then
    echo "scripts/start_native.sh: .venv is missing." >&2
    echo "Run scripts/install_native.sh first." >&2
    exit 1
fi

# Exported, not merely left in the file for the configuration layer to find.
# `mirobody dev` decides whether to generate JWT_KEY and CONFIG_ENCRYPTION_KEY
# BEFORE `Config.init` loads `.env`, so a value that only lives in the file is
# replaced on every start and no session survives a restart.
#
# Sourced before the port is read so an edit to `.env` is honoured here too.
if [[ -f .env ]]; then
    # A codespace created before 1.5.10 wrote `.env` as 666 (the default ACL on
    # `/workspaces` overrides the installer's umask), readable by the postgres
    # account. Narrowed here as well, since postStartCommand runs this on every
    # start and the installer never runs again. A `.env` someone else owns
    # cannot be changed, and that does not stop the start.
    chmod go-rwx .env 2>/dev/null || true
    set -a
    # shellcheck disable=SC1091
    . ./.env || echo "warning: .env could not be sourced; continuing with the environment as it is" >&2
    set +a
fi

HTTP_PORT="${HTTP_PORT:-18060}"

# Started by hand rather than through systemd, which a dev container does not
# run. Harmless where systemd already owns the cluster.
sudo pg_ctlcluster "${PG_VERSION}" main start 2>/dev/null || true

if curl -fsS "http://127.0.0.1:${HTTP_PORT}/api/health" >/dev/null 2>&1; then
    echo "Mirobody is already running on port ${HTTP_PORT}."
    exit 0
fi

# shellcheck disable=SC1091
. .venv/bin/activate

# Both commands run from the repository root on purpose. `config.yaml` is the
# defaults only while `config.llm.yaml` sits beside it, and the web client at
# `frontend/` is mounted only when it exists in the working directory: a
# `pip install` outside a checkout gets neither.
#
# `setsid` is load-bearing where a lifecycle hook runs this: the devcontainer
# runner cleans up the command's process group when the hook returns, and
# `nohup ... &` alone does not leave that group. Measured in a codespace, both
# logs were created and held zero bytes: postStartCommand reported success and
# nothing was left listening. A new session survives it, and `< /dev/null`
# takes the hook's stdin away from the servers.
setsid nohup mirobody worker config.yaml >"${LOG_DIR}/mirobody-worker.log" 2>&1 </dev/null &
setsid nohup mirobody dev config.yaml --host 0.0.0.0 --port "${HTTP_PORT}" \
    >"${LOG_DIR}/mirobody-server.log" 2>&1 </dev/null &

# Each forwarded port is served on its own domain, and the server's own hint can
# only name localhost (`server._print_setup_link`, which says so). Printed here
# instead.
#
# `CODESPACE_NAME` is not in the environment of every process in a codespace:
# measured, an SSH session and PID 1 both carry `CODESPACES` and not this. The
# two values are read from the file Codespaces writes them to, taking only those
# two keys because the same file holds GITHUB_TOKEN. On a machine that file is
# absent and the localhost address below is the right one.
codespace_env=/workspaces/.codespaces/shared/.env
if [[ -z "${CODESPACE_NAME:-}" && -r "${codespace_env}" ]]; then
    CODESPACE_NAME="$(sed -n 's/^CODESPACE_NAME=//p' "${codespace_env}" | head -1)"
    GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN="$(
        sed -n 's/^GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN=//p' "${codespace_env}" | head -1)"
fi

base="http://localhost:${HTTP_PORT}"
if [[ -n "${CODESPACE_NAME:-}" && -n "${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-}" ]]; then
    base="https://${CODESPACE_NAME}-${HTTP_PORT}.${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}"
fi

echo "Mirobody is starting."
echo "  app:    ${base}/"
if [[ -n "${SETUP_TOKEN:-}" ]]; then
    echo "  model:  ${base}/setup?token=${SETUP_TOKEN}"
fi
echo "  logs:   ${LOG_DIR}/mirobody-server.log"
echo "          ${LOG_DIR}/mirobody-worker.log"
echo

# Wait for the server, and this is load-bearing where a lifecycle hook runs
# this script rather than a person. Such a hook's parent is killed the moment
# the script returns, and a child that has not reached `exec` yet dies with it:
# upstream measures the log file created and empty, and prescribes a sleep
# (microsoft/vscode-remote-release#8536). Waiting for the health endpoint gives
# the children that time and reports the outcome, which a blind sleep cannot.
ready=""
for _ in $(seq 1 90); do
    if curl -fsS "http://127.0.0.1:${HTTP_PORT}/api/health" >/dev/null 2>&1; then
        ready=yes
        break
    fi
    sleep 1
done

if [[ -n "$ready" ]]; then
    echo "Mirobody is ready. The demo record is seeded; sign in as you@mirobody.ai,"
    echo "code 111111. A model key from the setup link above adds extraction and answers."
else
    echo "Mirobody did not answer on ${HTTP_PORT} within 90s; see the logs above." >&2
fi
