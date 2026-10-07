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
setting() { sed -n "s/^${1}=//p" .env | head -n 1; }

if ! has_setting ENV; then
    add_setting ENV "${ENV:-localdb}"
fi
env_name="${ENV:-$(setting ENV)}"
legacy_overlay="config.${env_name}.yaml"

# The overlay a pre-1.5.3 deploy wrote. The app encrypts any `*_KEY` or
# `*_PASSWORD` it finds there and rewrites the file, so a value may be a
# Fernet token (`gAAAA...`, sometimes folded onto the next line) that only the
# app can read. `overlay_plain` answers only for a value still in plain text.
overlay_defines() { [[ -f "$legacy_overlay" ]] && grep -qE "^${1}:" "$legacy_overlay"; }
overlay_plain() {
    awk -v name="$1" '$1 == name ":" {print $2; exit}' "$legacy_overlay" | tr -d "'\"" | grep -v '^gAAAA' || true
}

if [[ -f "$legacy_overlay" ]] && ! has_setting MIROBODY_CONFIG_FILE; then
    add_setting MIROBODY_CONFIG_FILE "./${legacy_overlay}"
fi
if ! has_setting MIROBODY_ENV_FILE; then
    add_setting MIROBODY_ENV_FILE './.env'
fi

# A 1.5.2 override names the redis service and mounts the checkout over /app,
# which in 1.5.3 is an invalid compose file or an image with its code hidden.
if [[ -f compose.override.yaml ]] \
    && grep -qE '^[[:space:]]*redis:|/root/venv|^[[:space:]]*-[[:space:]]*\.:/app' compose.override.yaml; then
    printf '%s\n' \
        'compose.override.yaml was written for 1.5.2: it names the redis service or mounts' \
        'this checkout over /app. 1.5.3 runs a built image with no Redis. Replace it:' \
        '    mv compose.override.yaml compose.override.yaml.1.5.2' \
        '    cp compose.override.yaml.example compose.override.yaml' \
        'and run ./deploy.sh again. docs/backup-restore.md, "Upgrading from 1.5.2", has the rest.' >&2
    exit 1
fi

# Compose's project name is the directory's, lowercased and stripped to
# [a-z0-9_-]: a checkout in `Mirobody/` keeps its database in volume
# `mirobody_mirobody_postgres`, and asking for `Mirobody_...` finds nothing.
project_name="${COMPOSE_PROJECT_NAME:-$(setting COMPOSE_PROJECT_NAME)}"
project_name="${project_name:-$(basename "$PWD")}"
project_name="$(printf '%s' "$project_name" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_-]//g')"

# Two checkouts under one project name are one stack to Compose: `up` here
# silently replaced the other checkout's containers and reused its database
# volume with this checkout's newly generated keys. A second clone called
# `mirobody` is the common way in (an agent following the skill picks it).
here="$(pwd -P)"
while IFS= read -r other; do
    [[ -z "$other" ]] && continue
    if [[ "$(cd "$other" 2>/dev/null && pwd -P || printf '%s' "$other")" != "$here" ]]; then
        printf 'A Mirobody stack named "%s" already runs from %s.\n' "$project_name" "$other" >&2
        printf 'Starting this checkout under the same name would take over its containers and its database.\n' >&2
        printf 'Run ./deploy.sh in %s, or give this checkout its own name and ports in .env:\n' "$other" >&2
        printf '    COMPOSE_PROJECT_NAME=%s-2\n    MIROBODY_HOST_PORT=18070\n    PG_HOST_PORT=18072\n' "$project_name" >&2
        exit 1
    fi
done < <(docker ps -a --filter "label=com.docker.compose.project=${project_name}" \
    --format '{{.Label "com.docker.compose.project.working_dir"}}' 2>/dev/null | sort -u)

data_dir="${MIROBODY_DATA:-$(setting MIROBODY_DATA)}"
data_dir="${data_dir:-../mirobody-data}"
existing_database=false
if docker volume inspect "${project_name}_mirobody_postgres" >/dev/null 2>&1; then
    existing_database=true
elif [[ -f compose.override.yaml && -d "${data_dir}/pgdata" ]] \
    && { [[ ! -r "${data_dir}/pgdata" ]] || [[ -n "$(ls -A "${data_dir}/pgdata")" ]]; }; then
    # The bind-mount override keeps the database outside any named volume. The
    # directory belongs to Postgres's uid, so unreadable counts as in use.
    existing_database=true
fi

ensure_secret() {
    local name=$1 plain
    if has_setting "$name"; then
        return
    fi
    if [[ -n "${!name:-}" ]]; then
        add_setting "$name" "${!name}"
        return
    fi
    if overlay_defines "$name"; then
        plain="$(overlay_plain "$name")"
        case "$name" in
            JWT_KEY|PG_ENCRYPTION_KEY)
                # The overlay is still mounted and the app decrypts it, so its
                # value stays in charge. A new one here would sign everyone out,
                # or leave every encrypted field unreadable.
                if [[ "$plain" != REPLACE_THIS_VALUE_IN_PRODUCTION ]]; then
                    return
                fi
                ;;
            *)
                # Needed before the app reads the overlay: CONFIG_ENCRYPTION_KEY
                # decrypts it, and compose hands PG_PASSWORD to Postgres and the app.
                if [[ -z "$plain" ]]; then
                    printf '%s in %s is encrypted, and compose needs it before the app can read that file.\n' "$name" "$legacy_overlay" >&2
                    printf 'Put its plain value in .env as %s=... and run ./deploy.sh again.\n' "$name" >&2
                    exit 1
                fi
                if [[ "$plain" != REPLACE_THIS_VALUE_IN_PRODUCTION ]]; then
                    add_setting "$name" "$plain"
                    return
                fi
                ;;
        esac
    fi
    if [[ "$existing_database" == true ]]; then
        case "$name" in
            PG_PASSWORD|PG_ENCRYPTION_KEY)
                # What a 1.5.2 stack used: compose's fixed database password,
                # and config.yaml's placeholder as the field-encryption key. Any
                # new value leaves the database unreachable or its encrypted
                # fields unreadable.
                add_setting "$name" REPLACE_THIS_VALUE_IN_PRODUCTION
                printf 'Existing database: %s keeps the placeholder value 1.5.2 used. See SECURITY.md before exposing this deployment.\n' "$name" >&2
                return
                ;;
        esac
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
# What the first-run page asks for before it changes where health data goes.
ensure_secret SETUP_TOKEN

# A model key, a local model server or a model name given on the command line
# (`OPENROUTER_API_KEY=... ./deploy.sh`, `LOCAL_BASE_URL=... LOCAL_MODEL=...
# ./deploy.sh`) goes into .env, the one file the containers read, so a first
# run needs no second step. The names are the ones config.llm.yaml reads
# (`api_key`, `base_url`, `model_env`), and a value already in .env is left as
# it is.
while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    if [[ -n "${!name:-}" ]] && ! has_setting "$name"; then
        add_setting "$name" "${!name}"
    fi
done < <(sed -nE 's/^[[:space:]]*(api_key|base_url|model_env):[[:space:]]*([A-Z][A-Z0-9_]*)[[:space:]]*(#.*)?$/\2/p' config.llm.yaml | sort -u)

# The model service started with the stack (`COMPOSE_PROFILES=local-cpu
# ./deploy.sh` with no GPU, `local` on an NVIDIA GPU) stays on for every later
# `docker compose up -d`, which reads .env and not this shell.
if [[ -n "${COMPOSE_PROFILES:-}" ]] && ! has_setting COMPOSE_PROFILES; then
    add_setting COMPOSE_PROFILES "$COMPOSE_PROFILES"
fi

# Ask the daemon, which is what pulls: the shell's proxy settings are not the
# daemon's, and hub.docker.com (the website) is not registry-1.docker.io. The
# smallest real image answers in one round trip.
registry_answers() { docker pull --quiet "${1}library/hello-world:latest" >/dev/null 2>&1; }
if ! has_setting DOCKER_MIRROR && [[ -z "${DOCKER_MIRROR:-}" ]] && ! registry_answers ""; then
    for mirror in docker.1ms.run/; do
        if registry_answers "$mirror"; then
            printf 'Docker Hub is unreachable from the daemon; using the mirror %s\n' "$mirror"
            add_setting DOCKER_MIRROR "$mirror"
            break
        fi
    done
fi

# By service, not by name: `--images mirobody` also lists the images it
# depends on (pg), and an image named anything (registry.example.com/app)
# must still be found. grep failing under `set -e` exited here silently.
pg_image="$(docker compose config --images pg)"
app_image="$(docker compose config --images mirobody | grep -vxF "$pg_image" | head -n 1 || true)"
if [[ -z "$app_image" ]]; then
    printf 'Could not tell which image compose.yaml runs as the mirobody service; check MIROBODY_IMAGE in .env.\n' >&2
    exit 1
fi
if ! docker image inspect "$app_image" >/dev/null 2>&1 && ! docker compose pull --quiet mirobody; then
    # Before a release is published, on a branch, or when no registry answers:
    # the checkout has everything the published image is built from.
    if head -c 40 mirobody/res/loinc/fhir_loinc_bundle.tar.gz | grep -q git-lfs; then
        printf 'Could not pull %s, and this checkout has Git LFS pointers instead of the\n' "$app_image" >&2
        printf 'terminology bundle, so it cannot be built here. Run: git lfs install && git lfs pull\n' >&2
        exit 1
    fi
    printf 'Could not pull %s. Building it from this checkout (several minutes the first time).\n' "$app_image"
    docker build -t "$app_image" \
        --build-arg "UBUNTU_IMAGE=${DOCKER_MIRROR:-$(setting DOCKER_MIRROR)}ubuntu:24.04" \
        ${PIP_INDEX_URL:+--build-arg "PIP_INDEX_URL=${PIP_INDEX_URL}"} .
fi

# A port another program holds ended the run in Docker's own words, naming
# neither the variable to change nor the file it goes in. Asked only when this
# stack is not already up: a re-run's own containers hold these ports.
if [[ -z "$(docker ps -q --filter "label=com.docker.compose.project=${project_name}" --filter status=running)" ]]; then
    for pair in "MIROBODY_HOST_PORT:18060" "PG_HOST_PORT:18062"; do
        var="${pair%%:*}"
        wanted="${!var:-$(setting "$var")}"
        wanted="${wanted:-${pair#*:}}"
        if (exec 3<>"/dev/tcp/127.0.0.1/${wanted}") 2>/dev/null; then
            printf 'Port %s is already in use on this machine.\n' "$wanted" >&2
            printf 'Pick a free one for %s in .env, e.g.  %s=%s  and run ./deploy.sh again.\n' \
                "$var" "$var" "$((wanted + 200))" >&2
            exit 1
        fi
    done
fi

printf 'Starting Mirobody with %s\n' "$app_image"
# --remove-orphans: a 1.5.2 stack's redis container is not part of 1.5.3.
docker compose up -d --wait --wait-timeout 600 --remove-orphans

for old in "${project_name}_mirobody_redis" "${project_name}_mirobody_site_packages"; do
    if docker volume inspect "$old" >/dev/null 2>&1; then
        printf 'Volume %s is from 1.5.2 and no longer used: docker volume rm %s\n' "$old" "$old"
    fi
done

port="${MIROBODY_HOST_PORT:-$(setting MIROBODY_HOST_PORT)}"
url="http://localhost:${port:-18060}"
# A key or server in .env, or a choice saved from the setup page earlier: only
# the app knows which, so it is asked rather than .env counted.
model_setup="$(curl -fsS "${url}/mirobody.json" 2>/dev/null \
    | sed -n 's/.*"__MODEL_SETUP__": *"\([a-z]*\)".*/\1/p' || true)"
case "$model_setup" in
    ready)
        printf '\nOpen %s\n' "$url"
        ;;
    needed)
        printf '\nChoose a model: open %s/setup?token=%s\n' "$url" "$(setting SETUP_TOKEN)"
        printf 'and paste one API key, or run every model on this machine with llama.cpp.\n'
        printf 'Or give it here instead, e.g.  OPENROUTER_API_KEY=sk-or-... ./deploy.sh\n'
        ;;
    *)
        printf '\nOpen %s\n' "$url"
        printf 'The app did not say whether it has a model yet; docker compose logs mirobody says why.\n' >&2
        ;;
esac
if [[ "${SEED_DEMO_DATA:-$(setting SEED_DEMO_DATA)}" != false ]]; then
    printf 'Demo sign-in: you@mirobody.ai, code 111111 on the Email code tab\n'
fi
