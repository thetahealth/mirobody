#!/usr/bin/env bash
#
# Start, stop or check the two local model servers 1.5.4 runs on:
#
#   GLM-OCR-0.9B   reads report photos and pages     :8081   LOCAL_OCR_BASE_URL
#   Bonsai-27B     the agent, titles and summaries   :8080   LOCAL_BASE_URL
#
# with the flags they were measured with (docs/local-models.md). The weights are
# not bundled and nothing here downloads them:
#
#   prism-ml/Ternary-Bonsai-2-27B-gguf   Ternary-Bonsai-2-27B-PTQ1_0.gguf, Ternary-Bonsai-2-27B-mmproj-Q8_0.gguf
#   ggml-org/GLM-OCR-GGUF                GLM-OCR-Q8_0.gguf, mmproj-GLM-OCR-Q8_0.gguf
#
# Bonsai's ternary GGUF needs PrismML's llama-server (github.com/PrismML-Eng/llama.cpp
# releases); stock llama.cpp cannot load PTQ1_0. The same binary runs GLM-OCR.
#
# Usage:
#   LLAMA_SERVER=~/llama-prism/llama-server MODELS_DIR=~/models shell/local-models.sh start
#   shell/local-models.sh status
#   shell/local-models.sh stop
#   HOST=172.17.0.1 ... start      # the app in Docker on Linux: listen on the bridge
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

LLAMA_SERVER="${LLAMA_SERVER:-$(command -v llama-server || true)}"
MODELS_DIR="${MODELS_DIR:-./models}"
RUN_DIR="${RUN_DIR:-./.theta/local-models}"
HOST="${HOST:-127.0.0.1}"
AGENT_PORT="${AGENT_PORT:-8080}"
OCR_PORT="${OCR_PORT:-8081}"

# name | port | model file | projector file | flags. The alias is the model id
# config.llm.yaml names, and `mirobody doctor --probe` checks the two agree.
SERVERS=(
  "glm-ocr|$OCR_PORT|GLM-OCR-Q8_0.gguf|mmproj-GLM-OCR-Q8_0.gguf|-c 16384 -np 2 --jinja -ngl 99"
  "bonsai-27b|$AGENT_PORT|Ternary-Bonsai-2-27B-PTQ1_0.gguf|Ternary-Bonsai-2-27B-mmproj-Q8_0.gguf|-c 65536 -np 2 --jinja -ngl 99 --reasoning on"
)

find_file() {
  local found
  found="$(find "$MODELS_DIR" -name "$1" -print -quit 2>/dev/null || true)"
  [ -n "$found" ] || { echo "not found under MODELS_DIR=$MODELS_DIR: $1 (see the list at the top of $0)" >&2; exit 1; }
  echo "$found"
}

start() {
  [ -x "$LLAMA_SERVER" ] || { echo "set LLAMA_SERVER to PrismML's llama-server binary" >&2; exit 1; }
  mkdir -p "$RUN_DIR"
  local name port model mmproj flags pid
  for entry in "${SERVERS[@]}"; do
    IFS='|' read -r name port model mmproj flags <<< "$entry"
    if [ -f "$RUN_DIR/$name.pid" ] && kill -0 "$(cat "$RUN_DIR/$name.pid")" 2>/dev/null; then
      echo "$name already running (pid $(cat "$RUN_DIR/$name.pid"))"
      continue
    fi
    model="$(find_file "$model")"; mmproj="$(find_file "$mmproj")"
    # shellcheck disable=SC2086
    "$LLAMA_SERVER" -m "$model" --mmproj "$mmproj" --host "$HOST" --port "$port" --alias "$name" $flags \
      > "$RUN_DIR/$name.log" 2>&1 &
    pid=$!
    echo "$pid" > "$RUN_DIR/$name.pid"
    for _ in $(seq 1 180); do
      curl -sf "http://$HOST:$port/health" >/dev/null && break
      kill -0 "$pid" 2>/dev/null || { echo "$name exited; see $RUN_DIR/$name.log" >&2; exit 1; }
      sleep 1
    done
    echo "$name on http://$HOST:$port/v1 (pid $pid, log $RUN_DIR/$name.log)"
  done
  echo
  echo "In .env (the app on this machine; in Docker use host.docker.internal):"
  echo "  LOCAL_BASE_URL=http://127.0.0.1:$AGENT_PORT/v1"
  echo "  LOCAL_OCR_BASE_URL=http://127.0.0.1:$OCR_PORT/v1"
}

stop() {
  local name
  for entry in "${SERVERS[@]}"; do
    name="${entry%%|*}"
    if [ -f "$RUN_DIR/$name.pid" ]; then
      kill "$(cat "$RUN_DIR/$name.pid")" 2>/dev/null && echo "stopped $name" || echo "$name was not running"
      rm -f "$RUN_DIR/$name.pid"
    fi
  done
}

status() {
  local name port served
  for entry in "${SERVERS[@]}"; do
    IFS='|' read -r name port _ <<< "$entry"
    served="$(curl -sf "http://$HOST:$port/v1/models" 2>/dev/null | sed -n 's/.*"id":"\([^"]*\)".*/\1/p' | head -1 || true)"
    echo "$name  :$port  ${served:-not answering}"
  done
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *) echo "usage: $0 start|stop|status" >&2; exit 2 ;;
esac
