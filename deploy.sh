#!/bin/bash

DOCKER_IMAGE_NAME=mirobody
DOCKER_COMPOSE_FILE="compose.yaml"
DOCKER_MIRRORS=("docker.1ms.run")

#-----------------------------------------------------------------------------
# Check local configure.

generate_random_string() {
  local length=${1:-16}
  local chars="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
  local random_str=""
  for (( i=0; i<length; i++ )); do
    random_str+=${chars:RANDOM%${#chars}:1}
  done
  echo "$random_str"
}

local_env=${ENV:-localdb}

# Check the .env file.
if [[ ! -f ".env" ]]; then
    echo "# Configuration set: any name you like. ENV only selects which
# config.\${ENV}.yaml overlay loads on top of config.yaml, and tags log lines.
# It carries NO behavior — production posture is declared with
# 'PRODUCTION: true' in the overlay, not by naming the ENV 'prod'.
ENV=${local_env}

# Encryption of sensitive configuration values.
CONFIG_ENCRYPTION_KEY=${CONFIG_ENCRYPTION_KEY:-$(generate_random_string 32)}

# Encryption of sensitive fields in log records (unset = ERROR noise at every
# boot and effectively-plaintext log fields).
LOG_ENCRYPTION_KEY=${LOG_ENCRYPTION_KEY:-$(generate_random_string 32)}

# ONE LLM API key is enough — uncomment one, paste the key, then
# 'docker compose restart'. Which model each key selects, per surface, is the
# table at the top of config.llm.yaml; the boot log prints what was selected.
#   OPENROUTER_API_KEY -> https://openrouter.ai/keys                  (recommended)
#   DASHSCOPE_API_KEY  -> https://dashscope.console.aliyun.com/apiKey (when openrouter.ai is unreachable)
#   GOOGLE_API_KEY     -> https://aistudio.google.com/apikey
#   OPENAI_API_KEY     -> https://platform.openai.com/api-keys
#   ANTHROPIC_API_KEY  -> https://platform.claude.com/settings/keys
#   DEEPSEEK_API_KEY   -> https://platform.deepseek.com/api_keys
# OPENROUTER_API_KEY=
# DASHSCOPE_API_KEY=
# GOOGLE_API_KEY=
# OPENAI_API_KEY=
# ANTHROPIC_API_KEY=
# DEEPSEEK_API_KEY=" > ".env"
    echo "Configure file '.env' has been created — put ONE LLM API key in it."
fi

# The three settings above used to be written ONLY when this script created
# `.env` itself. The common way a key reaches a machine is someone dropping in
# a `.env` that holds just the key — and then this script left it alone, so
# every compose command warned `The "ENV" variable is not set`,
# `config.${ENV}.yaml` (which holds the generated JWT_KEY) never loaded, and
# both encryption keys were missing, printing two ERROR lines at every boot.
# Append what is absent; never touch a value that is already there.
ensure_env_setting() {
    local name=$1 value=$2 comment=$3
    if grep -qE "^[[:space:]]*${name}=" ".env"; then
        return 0
    fi
    printf '\n# %s\n%s=%s\n' "${comment}" "${name}" "${value}" >> ".env"
    echo "Added missing ${name} to .env"
}

ensure_env_setting "ENV" "${local_env}" \
    "Which config.\${ENV}.yaml overlay loads on top of config.yaml. No behaviour of its own."
ensure_env_setting "CONFIG_ENCRYPTION_KEY" "${CONFIG_ENCRYPTION_KEY:-$(generate_random_string 32)}" \
    "Encryption of sensitive configuration values."
ensure_env_setting "LOG_ENCRYPTION_KEY" "${LOG_ENCRYPTION_KEY:-$(generate_random_string 32)}" \
    "Encryption of sensitive fields in log records."

# Check the config.{local_env}.yaml file.
config_filename="config.${local_env}.yaml"
if [ ! -f "${config_filename}" ]; then
    echo "# !IMPORTANT!
#   OVERRIDE DEFAULT SETTINGS IN THIS FILE.

#   ANY VALUE WHOSE NAME CONTAINS 
#     '_KEY', '_PASSWORD', '_PASS', '_PWD', '_SECRET', '_SK', or '_TOKEN'
#     WILL BE ENCRYPTED AUTOMATICALLY.
#   YOU CAN SPECIFY THE ENCRYPTION KEY VIA ENVIRONMENT VARIABLE 'CONFIG_ENCRYPTION_KEY'.


# ============================================================================
# Authentication & Security
# ============================================================================

# OAuth JWT signature key (256 bits / 32 bytes).
# Used for signing and verifying JWT tokens in OAuth authentication flow.
# Generate a secure random key using: openssl rand -hex 32
JWT_KEY: $(generate_random_string 32)

# Predefined email addresses and verification codes for testing purposes.
# The demo logins themselves (you@mirobody.ai and mom@mirobody.ai, code
# 111111) ship in config.yaml — do NOT redeclare EMAIL_PREDEFINE_CODES here
# unless you mean to REPLACE it: overlay dicts substitute the whole key, they
# do not merge (an earlier generated block did exactly that, and the README's
# stated login stopped working on every ./deploy.sh install). To ADD accounts,
# list the demo lines again alongside yours:
# EMAIL_PREDEFINE_CODES:
#   you@mirobody.ai: '111111'
#   mom@mirobody.ai: '111111'
#   me@example.com: '<your code>'
#
# BEFORE EXPOSING THIS DEPLOYMENT TO A NETWORK, set the two lines below and
# remove the predefined codes (see SECURITY.md). With PRODUCTION: true the
# server REFUSES to start while predefined codes or placeholder secrets remain.
# PRODUCTION: true
# BOOTSTRAP_SCHEMA: false


# ============================================================================
# LLM
# ============================================================================

# The API key goes in .env (next to compose.yaml), not here — one place.
# Which model each surface uses is config.llm.yaml; override a route here if
# you must, e.g.:
# UTILS_VISION_MODEL: qwen-utils


# ============================================================================
# Network Configuration
# ============================================================================

# MCP (Model Context Protocol) public URL.
# This must be a publicly accessible domain so that LLMs can visit this server.
#
# Setup instructions if you don't have a public domain yet:
#   1. Visit https://ngrok.com/docs/getting-started and install ngrok.
#   2. Run 'ngrok http 18060' in your terminal.
#   3. Copy your ngrok domain (e.g., https://abc123.ngrok-free.app).
#   4. Set MCP_PUBLIC_URL to it.
#
# Example format:
#   MCP_PUBLIC_URL: 'https://abc123.ngrok-free.app'
#   or
#   MCP_PUBLIC_URL: 'https://yourdomain.com'
# MCP_PUBLIC_URL: 'YOUR PUBLIC DOMAIN NAME WIH SCHEMA'" > "${config_filename}"
    echo "Configure file '${config_filename}' has been created."
fi


#-----------------------------------------------------------------------------
# Determine docker mirror.

# Ask the process that actually pulls images.
#
# This used to be a `curl https://hub.docker.com` from THIS SHELL, and it was
# wrong twice over:
#
#   1. the shell has `http_proxy`/`https_proxy`; the daemon does not inherit
#      them. On a proxied machine curl returned 200, no mirror was selected,
#      and `compose up` then died inside the daemon with
#      `Get "https://registry-1.docker.io/v2/": ... Client.Timeout`. The
#      fallback failed in exactly the networks that need it.
#   2. `hub.docker.com` is the WEBSITE. Images come from
#      `registry-1.docker.io`, a different host that can be blocked on its own.
#
# So pull the smallest real image there is (~20 kB) through the daemon. It
# costs one round trip on a healthy network and is the only answer that
# predicts what `docker compose up` is about to do.
check_registry_through_daemon() {
    local prefix=$1 label=$2
    echo "Checking ${label} by pulling through the docker daemon (this is the check that counts) ..."
    docker pull --quiet "${prefix}library/hello-world:latest" &>/dev/null
}

docker_host=""
if ! check_registry_through_daemon "" "Docker Hub"; then
    echo "The docker daemon cannot reach Docker Hub. Trying the mirrors ..."
    for host in "${DOCKER_MIRRORS[@]}"; do
        if check_registry_through_daemon "${host}/" "mirror ${host}"; then
            echo "Using docker mirror: ${host}"
            docker_host="${host}/"
            break
        fi
    done
    if [[ -z "${docker_host}" ]]; then
        echo "WARNING: neither Docker Hub nor any mirror answered the daemon." >&2
        echo "         The build below will most likely fail on the first pull." >&2
        echo "         If this machine reaches the internet through a proxy, the DAEMON" >&2
        echo "         needs it too — on Docker Desktop that is Settings > Resources >" >&2
        echo "         Proxies; with systemd it is a drop-in for docker.service." >&2
    fi
fi

#-----------------------------------------------------------------------------
# Build docker image.

# curl stays: compose.yaml's app healthcheck shells out to it. The former
# nodesource setup_24.x line is gone on purpose — it added Node's apt repo
# (hence gnupg) but nodejs was never in the install list, nothing at runtime
# executes node, and the frontend ships prebuilt static files served by the
# app itself. It only cost build time and a curl|bash.
mirobody_dockerfile_content="
FROM ${docker_host}ubuntu:24.04
RUN apt update && \
    apt install -y --no-install-recommends \
        ca-certificates curl \
        g++ gfortran build-essential \
        libfftw3-dev libhdf5-dev libblas-dev liblapack-dev \
        python3 python3-venv python3-dev \
        fonts-wqy-microhei fonts-wqy-zenhei fontconfig && \
    rm -rf /var/lib/apt/lists/* && \
    fc-cache -fv && \
    mkdir /root/venv && \
    python3 -m venv /root/venv && \
    mkdir -p /app
# The venv on PATH, so an interactive shell is the same interpreter the service
# runs. Without it \`docker compose exec mirobody python -m mirobody doctor\` --
# printed by this script after every successful deploy, and asked for five
# times by .github/ISSUE_TEMPLATE/upload-no-indicators.yml -- answers
# \`python: command not found\`, and \`python3\` finds the system interpreter
# with none of our dependencies. The compose commands source the venv
# themselves, so the service never noticed. (2026-09-14 regression report, F-1)
ENV PATH=/root/venv/bin:\$PATH
WORKDIR /app
"
mirobody_dockerfile_version=$(echo -n "${mirobody_dockerfile_content}" | openssl md5 | awk '{print $NF}')

existing_mirobody_dockerfile_version=$(docker image inspect --format '{{ index .Config.Labels "dockerfile.md5" }}' ${DOCKER_IMAGE_NAME} 2>/dev/null)
if [ "${mirobody_dockerfile_version}" = "${existing_mirobody_dockerfile_version}" ]; then
    echo "Using existing docker image."
else
    echo "Building docker image ..."
    echo -e "${mirobody_dockerfile_content}" | docker build -t "${DOCKER_IMAGE_NAME}" --label "dockerfile.md5=${mirobody_dockerfile_version}" -
fi

#-----------------------------------------------------------------------------
# Run docker containers.

# check_ports_free {ports}
#
# Refuse to proceed when a required host port is held by a FOREIGN container.
# This used to `docker container stop` whatever held the port — which on a
# shared machine silently took down other projects' databases. Our own
# containers are already gone by now (`compose down` above), so anything still
# on these ports belongs to someone else: name it and let the operator decide.
check_ports_free() {
    local conflict=0
    for port in "$@"; do
        local holder
        holder=$(docker ps --format '{{.Names}}\t{{.Ports}}' | grep ":$port->" | cut -f1)
        if [ -n "${holder}" ]; then
            echo "ERROR: port ${port} is held by container '${holder}' (not part of this project)."
            conflict=1
        fi
    done
    if [ ${conflict} -eq 1 ]; then
        echo ""
        echo "Refusing to stop containers that don't belong to this deployment."
        echo "Free the ports (or change this project's ports) and re-run deploy.sh."
        exit 1
    fi
}

# check_subnet_free
#
# compose.yaml pins mirobody_network to 10.108.0.0/24 (config.yaml resolves
# pg/redis by fixed addresses inside it). If another Docker network already
# holds that subnet — a second checkout of this repo, or an unrelated project —
# `compose up` dies with "Pool overlaps with other one on this address space",
# which names neither the network nor the fix. Name both, up front. Our own
# network is gone by now (`compose down` above), so any holder is foreign.
check_subnet_free() {
    # Read the subnet from compose.yaml rather than hardcoding it, so the
    # recovery advice below actually works: an operator who moves the stack to
    # another subnet must not be re-rejected by a guard still checking the old
    # one (that happened — the guard and the file disagreed).
    local subnet
    subnet=$(awk '/subnet:/ {print $NF; exit}' "${DOCKER_COMPOSE_FILE}")
    subnet="${subnet:-10.108.0.0/24}"
    # Exclude only THIS project's own network, by exact name: compose reuses a
    # same-name network without conflict, while ANY other holder — including a
    # second checkout of this repo — collides. A substring filter on
    # "mirobody" would blind the check to exactly that second-checkout case.
    local own
    own="$(basename "$PWD" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]//g')_mirobody_network"
    local holder
    holder=$(docker network ls -q | xargs -r docker network inspect \
        --format '{{.Name}} {{json .IPAM.Config}}' 2>/dev/null \
        | grep -F "${subnet}" | awk -v own="${own}" '$1 != own {print $1}' | head -1)
    if [ -n "${holder}" ]; then
        echo "ERROR: Docker network '${holder}' already uses ${subnet}, which compose.yaml pins for this stack."
        echo ""
        echo "Either remove that network if it is unused:  docker network rm ${holder}"
        echo "or move this stack to a free subnet: edit mirobody_network in compose.yaml"
        echo "AND point PG_HOST / REDIS_HOST at the new pg/redis addresses in your overlay."
        exit 1
    fi
}

# compose.yaml interpolates ${DOCKER_MIRROR:-} onto its pulled images
# (pgvector, redis), so the mirror fallback covers them too — previously it
# only applied to the inline-built ubuntu base, and a first deploy behind the
# mirror failed pulling pg/redis from the unreachable hub.
export DOCKER_MIRROR="${docker_host}"

# Terminology data that is not in the checkout, listed in
# mirobody/res/EXTERNAL.tsv. Since 1.5.0 every row there is an archive row and
# nothing is downloaded; the call stays for a release that adds one. Never fatal.
if [[ -x "$(dirname "$0")/scripts/fetch_data.sh" ]]; then
    "$(dirname "$0")/scripts/fetch_data.sh"
fi

# No `-f`: naming the file turns OFF Compose's automatic pickup of
# `compose.override.yaml`, which is where a rootless-Docker host puts its
# bind mounts — and the commands this script prints for the user
# (`docker compose restart`) DO load it. Two paths, two stacks, and the
# failure was silent: the override was ignored, the named volumes it
# replaces were rejected, and the script still said "Up".
docker compose down
check_ports_free 18060 18062 18069
check_subnet_free

up_log="$(mktemp)"
if ! (set -o pipefail; docker compose up -d --remove-orphans 2>&1 | tee "${up_log}"); then
    if grep -q "Host path binding is rejected" "${up_log}"; then
        # README promises the fix is in the message; compose's own line is not it.
        echo ""
        echo "This Docker daemon refuses named volumes (rootless or hardened). Use bind mounts:"
        echo ""
        echo "    cp compose.override.yaml.example compose.override.yaml"
        echo "    mkdir -p ../mirobody-data/{pgdata,redis,upload,sitepkgs}"
        echo "    docker compose down -v    # the empty volumes this attempt created"
        echo "    ./deploy.sh"
        rm -f "${up_log}"
        exit 1
    fi
    rm -f "${up_log}"
    # "nothing is running" was wrong and sent people the wrong way: compose
    # starts pg and redis, then fails on mirobody, and the real cause (a pip
    # install that could not reach an index, a rejected volume) is only in
    # that container's log. An outside deployment report measured three
    # containers Up under this message.
    echo ""
    echo "Compose could not finish. Some containers may be up and not ready:"
    docker compose ps 2>/dev/null || true
    echo ""
    echo "The cause is usually in the mirobody container's own log:"
    docker compose logs --tail 40 mirobody 2>/dev/null || true
    echo ""
    echo "Nothing below this line ran. Fix the error and re-run ./deploy.sh."
    exit 1
fi
rm -f "${up_log}"
echo ""
echo "Up. Open http://localhost:18060 and, on the Email code tab, sign in as you@mirobody.ai / 111111."
echo "The boot log below ends with 'LLM models by surface', or, while no key is"
echo "set at all, with 'no LLM model on any surface'. For either of those, and for"
echo "a surface that reads '--', put ONE LLM API key in .env (the names are listed"
echo "there) and run:"
echo "    docker compose restart"
echo "Details any time:  docker compose exec mirobody python -m mirobody doctor"
echo ""
docker compose logs -f

#-----------------------------------------------------------------------------
