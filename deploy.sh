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
#   DEEPSEEK_API_KEY   -> https://platform.deepseek.com/api_keys
# OPENROUTER_API_KEY=
# DASHSCOPE_API_KEY=
# GOOGLE_API_KEY=
# OPENAI_API_KEY=
# DEEPSEEK_API_KEY=" > ".env"
    echo "Configure file '.env' has been created — put ONE LLM API key in it."
fi

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
# The demo login itself (caregiver@mirobody.ai / 111111) ships in config.yaml —
# do NOT redeclare EMAIL_PREDEFINE_CODES here unless you mean to REPLACE it:
# overlay dicts substitute the whole key, they do not merge (an earlier
# generated block did exactly that, and the README's stated login stopped
# working on every ./deploy.sh install). To ADD accounts, list the caregiver
# line again alongside yours:
# EMAIL_PREDEFINE_CODES:
#   caregiver@mirobody.ai: '111111'
#   you@example.com: '<your code>'
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

check_connection_by_hostname() {
    local host=$1
    local timeout=3
    local http_status=0
    if command -v curl &>/dev/null; then
        echo "Checking '${host}' via curl ..."
        http_status=$(curl --connect-timeout ${timeout} -o /dev/null -s -w "%{http_code}" https://${host})
    elif command -v wget &>/dev/null; then
        echo "Checking '${host}' via wget ..."
        http_status=$(wget -T 3 --spider --tries=1 --server-response "${host}" 2>&1 | grep "HTTP/" | awk '{print $2}' | tail -n 1)
    else
        echo "Checking '${host}' via ping ..."
        if ping -c 3 ${host} &> /dev/null; then
            return 0
        fi
    fi
    case "${http_status}" in
        200|301|302)
            return 0
            ;;
    esac
    return 1
}

docker_host=""
if ! check_connection_by_hostname "hub.docker.com"; then
    for host in "${DOCKER_MIRRORS[@]}"; do
        if check_connection_by_hostname "${host}"; then
            echo "Using docker mirror: ${host}"
            docker_host="${host}/"
            break
        fi
    done
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

docker compose -f ${DOCKER_COMPOSE_FILE} down
check_ports_free 18060 18062 18069
check_subnet_free

docker compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans
echo ""
echo "Up. Open http://localhost:18060 and sign in as caregiver@mirobody.ai / 111111."
echo "The boot log below ends with 'LLM providers by surface': if a surface reads '--',"
echo "put ONE LLM API key in .env (the names are listed there) and run:"
echo "    docker compose restart"
echo "Details any time:  docker compose exec mirobody python -m mirobody doctor"
echo ""
docker compose -f ${DOCKER_COMPOSE_FILE} logs -f

#-----------------------------------------------------------------------------
