# deploy.sh passes a mirror's ubuntu when the daemon cannot reach Docker Hub,
# and PIP_INDEX_URL for a slow PyPI.
ARG UBUNTU_IMAGE=ubuntu:24.04
FROM ${UBUNTU_IMAGE} AS builder
ARG PIP_INDEX_URL=https://pypi.org/simple
ENV PIP_INDEX_URL=${PIP_INDEX_URL}

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates build-essential g++ gfortran \
    libfftw3-dev libhdf5-dev libblas-dev liblapack-dev \
    python3 python3-dev python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH
WORKDIR /src
COPY pyproject.toml MANIFEST.in README.md CHANGELOG.md SECURITY.md LICENSE LICENSE-3RD-PARTY ./
COPY config.yaml config.llm.yaml config.devices.yaml ./
COPY docker/constraints.txt docker/constraints.txt
COPY scripts/build_backend.py scripts/build_backend.py
ARG MIROBODY_VERSION=1.5.3.dev0
# Resolve the full application extra before copying the source. Code changes
# then rebuild only the small project wheel, not every native dependency.
RUN mkdir mirobody \
    && printf '__version__ = "%s"\n' "$MIROBODY_VERSION" > mirobody/__init__.py
RUN --mount=type=cache,target=/root/.cache/pip \
    MIROBODY_VERSION="$MIROBODY_VERSION" pip install \
    --timeout 120 --retries 5 --constraint docker/constraints.txt '.[app]'
COPY mirobody/ mirobody/
# The install above left `build/lib` holding the version stub, newer than the
# COPY'd source, so the next build skipped `mirobody/__init__.py` and shipped a
# 22-byte package: `import mirobody` works, `mirobody.resolve` raises ImportError.
RUN rm -rf build *.egg-info
RUN --mount=type=cache,target=/root/.cache/pip \
    MIROBODY_VERSION="$MIROBODY_VERSION" pip install --no-deps --force-reinstall .
# Import outside /src so the real installed wheel is checked, not the checkout.
# Both platform builds must reject a cached stub before publishing the image.
RUN cd / && python -c \
    'import mirobody; from mirobody import resolve; assert mirobody.BUNDLE_VERSION; assert resolve("hemoglobin").loinc == "718-7"'

FROM ${UBUNTU_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl fontconfig fonts-wqy-microhei fonts-wqy-zenhei \
    libfftw3-double3 libhdf5-103-1t64 libblas3 liblapack3 libgfortran5 \
    python3 python3-venv \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY config.yaml config.llm.yaml config.devices.yaml ./
COPY LICENSE LICENSE-3RD-PARTY ./
COPY frontend/ frontend/
# The documents the demo seed files for its two accounts. The package lives in
# /opt/venv, so the seed cannot find them beside itself.
COPY demo/seed/ demo/seed/
ENV DEMO_DATA_DIR=/app/demo
# A fixed uid AND gid: bind mounts and restored backups are owned by number,
# and `mirobody_init` in compose.yaml hands the upload volume to this user.
RUN groupadd --system --gid 10001 mirobody \
    && useradd --system --uid 10001 --gid 10001 --create-home mirobody \
    && mkdir -p /app/.theta/mcp/upload \
    && chown -R mirobody:mirobody /app
USER mirobody

EXPOSE 18060
HEALTHCHECK --interval=10s --timeout=5s --start-period=120s --retries=30 \
    CMD curl -fsS http://127.0.0.1:18060/api/health >/dev/null || exit 1
CMD ["mirobody", "serve"]
