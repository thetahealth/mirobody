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
    echo "# Configuartion sets.
# 'localdb', 'test', 'gray', 'prod', or a name defined by yourself.
ENV=${local_env}

# Encryption of sensitive configuration values.
CONFIG_ENCRYPTION_KEY=${CONFIG_ENCRYPTION_KEY:-$(generate_random_string 32)}" > ".env"
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
EMAIL_PREDEFINE_CODES:
  demo1@mirobody.ai: '777777'
  demo2@mirobody.ai: '777777'
  demo3@mirobody.ai: '777777'


# ============================================================================
# AI Service API Keys
# ============================================================================

# Google Gemini API key.
# Required for Google Gemini AI model integration.
# Get your API key from: https://makersuite.google.com/app/apikey
GOOGLE_API_KEY: ''

# OpenAI API key.
# Required for OpenAI model integration (GPT-5, etc.).
# Get your API key from: https://platform.openai.com/api-keys
OPENAI_API_KEY: ''

# OpenRouter API key.
# Required for accessing multiple AI models through OpenRouter service.
# Get your API key from: https://openrouter.ai/keys
OPENROUTER_API_KEY: ''


# ============================================================================
# Network Configuration
# ============================================================================

# MCP (Model Context Protocol) public URL.
# This must be a publicly accessible domain so that LLMs can visit this server.
#
# Setup instructions if you don't have a public domain yet:
#   1. Visit https://ngrok.com/docs/getting-started and install ngrok.
#   2. Run 'ngrok http 18080' in your terminal.
#   3. Copy your ngrok domain (e.g., https://abc123.ngrok-free.app).
#   4. Set MCP_PUBLIC_URL to it.
#
# Example format:
#   MCP_PUBLIC_URL: 'https://abc123.ngrok-free.app'
#   or
#   MCP_PUBLIC_URL: 'https://yourdomain.com'
MCP_PUBLIC_URL: ''" > "${config_filename}"
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

mirobody_dockerfile_content="
FROM ${docker_host}ubuntu:24.04
RUN apt update && \
    apt install -y ca-certificates curl gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_24.x | bash - && \
    apt install -y --no-install-recommends \
        g++ gfortran build-essential \
        libfftw3-dev libhdf5-dev libblas-dev liblapack-dev \
        python3 python3-venv python3-dev \
        nodejs \
        fonts-wqy-microhei fonts-wqy-zenhei fontconfig && \
    fc-cache -fv && \
    mkdir /root/venv && \
    python3 -m venv /root/venv && \
    npm config set registry https://registry.npmmirror.com && \
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

# stop_containers_by_ports {ports}
stop_containers_by_ports() {
    for port in $@; do
        results=($(docker ps | grep ":$port->"))
        if [ ${#results[@]} -gt 0 ]; then
            echo "docker container stop ${results[0]}"
            docker container stop ${results[0]}
        fi
    done
}

docker compose -f ${DOCKER_COMPOSE_FILE} down
stop_containers_by_ports 18080 18082 18089

docker compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans pg
docker compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans redis
docker compose -f ${DOCKER_COMPOSE_FILE} up -d --remove-orphans mirobody
docker compose -f ${DOCKER_COMPOSE_FILE} logs -f

#-----------------------------------------------------------------------------
