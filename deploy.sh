#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
umask 077
if ! command -v docker >/dev/null 2>&1; then
    printf 'Docker is required to run Mirobody.\n' >&2
    exit 1
fi

touch .env
chmod 600 .env

has_setting() { grep -q "^${1}=" .env; }
add_setting() { printf '%s=%s\n' "$1" "$2" >> .env; }

if ! has_setting ENV; then
    add_setting ENV "${ENV:-localdb}"
fi
env_name="${ENV:-$(sed -n 's/^ENV=//p' .env | head -n 1)}"
legacy_overlay="config.${env_name}.yaml"

legacy_value() {
    if [[ ! -f "$legacy_overlay" ]]; then
        return
    fi
    awk -v name="$1" '$1 == name ":" {print $2; exit}' "$legacy_overlay" | tr -d "'\""
}

if [[ -f "$legacy_overlay" ]] && ! has_setting MIROBODY_CONFIG_FILE; then
    add_setting MIROBODY_CONFIG_FILE "./${legacy_overlay}"
fi
if ! has_setting MIROBODY_ENV_FILE; then
    add_setting MIROBODY_ENV_FILE './.env'
fi

project_name="${COMPOSE_PROJECT_NAME:-$(sed -n 's/^COMPOSE_PROJECT_NAME=//p' .env | head -n 1)}"
project_name="${project_name:-$(basename "$PWD")}"
existing_database=false
if docker volume inspect "${project_name}_mirobody_postgres" >/dev/null 2>&1; then
    existing_database=true
fi

ensure_secret() {
    local name=$1 old_value
    if has_setting "$name"; then
        return
    fi
    if [[ -n "${!name:-}" ]]; then
        add_setting "$name" "${!name}"
        return
    fi
    old_value="$(legacy_value "$name")"
    if [[ -n "$old_value" && "$old_value" != REPLACE_THIS_VALUE_IN_PRODUCTION ]]; then
        # The old deploy script kept JWT and database encryption in the YAML
        # overlay; changing either would strand existing data or sessions.
        if [[ "$name" == CONFIG_ENCRYPTION_KEY || "$name" == LOG_ENCRYPTION_KEY || "$name" == PG_PASSWORD ]]; then
            add_setting "$name" "$old_value"
        fi
        return
    fi
    if [[ "$existing_database" == true && "$name" == PG_PASSWORD ]]; then
        # The pre-1.5.3 Compose service used this fixed database password.
        add_setting PG_PASSWORD REPLACE_THIS_VALUE_IN_PRODUCTION
        return
    fi
    if [[ "$existing_database" == true && "$name" == PG_ENCRYPTION_KEY ]]; then
        printf 'Existing database found. Restore its PG_ENCRYPTION_KEY from the old config overlay into .env before upgrading.\n' >&2
        exit 1
    fi
    if ! command -v openssl >/dev/null 2>&1; then
        printf 'OpenSSL is required to generate %s. Set it in .env and retry.\n' "$name" >&2
        exit 1
    fi
    add_setting "$name" "$(openssl rand -hex 32)"
}

ensure_secret PG_PASSWORD
ensure_secret PG_ENCRYPTION_KEY
ensure_secret CONFIG_ENCRYPTION_KEY
ensure_secret LOG_ENCRYPTION_KEY
ensure_secret JWT_KEY

printf 'Starting Mirobody with %s\n' "${MIROBODY_IMAGE:-thetahealth/mirobody:1.5.3}"
docker compose up -d --wait --wait-timeout 600
printf '\nOpen http://localhost:%s\n' "${MIROBODY_HOST_PORT:-18060}"
printf 'Demo sign-in: you@mirobody.ai / 111111\n'
printf 'Set one model API key in .env, then run: docker compose up -d\n'
