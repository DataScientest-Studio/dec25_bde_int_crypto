#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
PROJECT_NAME="dec25_bde_int_crypto"
STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-180}"
BUILD_IMAGES=0
PRUNE_IMAGES=0
FRESH_DATA=0
MONGO_REPAIRED=0
USE_CURL=0
LAST_UP_LOG=""

DEFAULT_SERVICES=(
  mongodb
  redpanda-0
  console
  mongo-express
  main
  grafana
  binance-collector
  stream-producer
  stream-consumer
  prediction-api
  dashboard
)

log() {
  printf '[docker-up-clean] %s\n' "$*"
}

warn() {
  printf '[docker-up-clean] WARN: %s\n' "$*" >&2
}

die() {
  printf '[docker-up-clean] ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: ./scripts/docker-up-clean.sh [options]

Starts the long-running Docker services for this project with a safer startup
flow: cache cleanup, stale container cleanup, health checks, and MongoDB repair
for the empty-volume bootstrap edge case. If prediction model artifacts are
missing, the script trains them and restarts the prediction services.

Options:
  --build          Rebuild images before starting containers.
  --prune-images   Remove unused Docker images before startup.
  --fresh-data     Remove this project's MongoDB and Redpanda volumes first.
                   This resets persisted data for those services.
  --timeout N      Startup timeout in seconds. Default: 180
  -h, --help       Show this help message.
EOF
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

compose() {
  (
    cd "$PROJECT_ROOT"
    docker compose "$@"
  )
}

container_state() {
  docker inspect \
    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
    "$1" 2>/dev/null || true
}

service_logs_contain() {
  compose logs "$1" --tail=80 2>/dev/null | grep -qi -- "$2"
}

show_service_logs() {
  compose logs "$1" --tail=80 >&2 || true
}

container_is_running() {
  [ "$(container_state "$1")" = "running" ] || [ "$(container_state "$1")" = "healthy" ]
}

ensure_prerequisites() {
  require_command docker
  require_command mkdir
  require_command tee

  if command -v curl >/dev/null 2>&1; then
    USE_CURL=1
  fi

  docker info >/dev/null 2>&1 || die "Docker is not reachable. Start Docker Desktop and try again."
  compose config >/dev/null 2>&1 || die "docker compose configuration is invalid."

  if [ ! -f "$PROJECT_ROOT/.env" ]; then
    die "Missing .env file at $PROJECT_ROOT/.env"
  fi

  mkdir -p "$PROJECT_ROOT/data/raw_data" "$PROJECT_ROOT/data/processed_data"
}

prune_build_cache() {
  log "Cleaning unused Docker build cache."
  docker builder prune -af >/dev/null
}

prune_unused_images() {
  log "Cleaning unused Docker images."
  docker image prune -af >/dev/null
}

stop_existing_stack() {
  log "Stopping old containers and removing orphans for this project."
  compose down --remove-orphans >/dev/null 2>&1 || true
}

reset_state_volumes() {
  log "Removing persisted MongoDB and Redpanda volumes for a fresh start."
  docker volume rm -f \
    "${PROJECT_NAME}_mongodb_data" \
    "${PROJECT_NAME}_redpanda-0" >/dev/null 2>&1 || true
}

run_compose_up() {
  local log_file
  local args=(up -d)

  if [ "$BUILD_IMAGES" -eq 1 ]; then
    args+=(--build)
  fi

  args+=("${DEFAULT_SERVICES[@]}")

  log "Launching the Docker stack."
  log_file="$(mktemp)"

  if compose "${args[@]}" 2>&1 | tee "$log_file"; then
    LAST_UP_LOG="$log_file"
    return 0
  fi

  LAST_UP_LOG="$log_file"
  return 1
}

startup_hit_disk_pressure() {
  if [ -n "$LAST_UP_LOG" ] && grep -qi 'no space left on device' "$LAST_UP_LOG"; then
    return 0
  fi

  if service_logs_contain mongodb 'no space left on device'; then
    return 0
  fi

  if service_logs_contain redpanda-0 'no space left on device'; then
    return 0
  fi

  return 1
}

repair_empty_mongodb_auth() {
  local db_count

  [ "$MONGO_REPAIRED" -eq 0 ] || return 1

  if ! service_logs_contain mongodb 'Could not find user "admin" for db "admin"'; then
    return 1
  fi

  db_count="$(
    docker exec mongodb mongosh --quiet \
      --eval 'db.adminCommand({listDatabases: 1}).databases.length' 2>/dev/null || true
  )"

  if [ "$db_count" != "0" ]; then
    warn "MongoDB looks stuck on auth, but the data volume is not empty. Leaving persisted data untouched."
    return 1
  fi

  log "Repairing empty MongoDB volume by recreating the missing admin user."
  docker exec mongodb mongosh --quiet --eval \
    'db.getSiblingDB("admin").createUser({user:"admin", pwd:"password", roles:[{role:"root", db:"admin"}]})' >/dev/null
  MONGO_REPAIRED=1
  return 0
}

wait_for_redpanda() {
  local deadline state
  deadline=$((SECONDS + STARTUP_TIMEOUT))

  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(container_state redpanda-0)"

    case "$state" in
      running|healthy)
        return 0
        ;;
      exited|dead)
        show_service_logs redpanda-0
        die "Redpanda exited during startup."
        ;;
      *)
        if service_logs_contain redpanda-0 'no space left on device'; then
          return 20
        fi
        sleep 2
        ;;
    esac
  done

  show_service_logs redpanda-0
  die "Timed out waiting for Redpanda to start."
}

wait_for_mongodb() {
  local deadline state
  deadline=$((SECONDS + STARTUP_TIMEOUT))

  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(container_state mongodb)"

    case "$state" in
      healthy)
        return 0
        ;;
      exited|dead)
        show_service_logs mongodb
        die "MongoDB exited during startup."
        ;;
      *)
        if service_logs_contain mongodb 'no space left on device'; then
          return 20
        fi
        repair_empty_mongodb_auth || true
        sleep 2
        ;;
    esac
  done

  show_service_logs mongodb
  die "Timed out waiting for MongoDB to become healthy."
}

wait_for_main_api() {
  local deadline state
  deadline=$((SECONDS + STARTUP_TIMEOUT))

  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(container_state main)"

    if [ "$state" = "healthy" ]; then
      return 0
    fi

    if [ "$USE_CURL" -eq 1 ] && curl -fsS http://localhost:8000/ >/dev/null 2>&1; then
      return 0
    fi

    case "$state" in
      exited|dead)
        show_service_logs main
        die "The main API container exited during startup."
        ;;
      *)
        sleep 2
        ;;
    esac
  done

  show_service_logs main
  die "Timed out waiting for the main API to become ready."
}

wait_for_stack() {
  wait_for_redpanda || return $?
  wait_for_mongodb || return $?
  wait_for_main_api || return $?
}

prediction_api_ready() {
  docker exec prediction-api python -c \
    "import json, urllib.request; data=json.load(urllib.request.urlopen('http://localhost:8000/predict/logistic/status/check', timeout=5)); raise SystemExit(0 if data.get('model_loaded') and data.get('scaler_loaded') else 1)" \
    >/dev/null 2>&1
}

wait_for_prediction_api_ready() {
  local deadline state
  deadline=$((SECONDS + STARTUP_TIMEOUT))

  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(container_state prediction-api)"

    if prediction_api_ready; then
      return 0
    fi

    case "$state" in
      exited|dead)
        show_service_logs prediction-api
        die "The prediction API container exited during startup."
        ;;
      *)
        sleep 2
        ;;
    esac
  done

  show_service_logs prediction-api
  die "Timed out waiting for the prediction API model to become ready."
}

wait_for_prediction_api_container() {
  local deadline state
  deadline=$((SECONDS + STARTUP_TIMEOUT))

  while [ "$SECONDS" -lt "$deadline" ]; do
    state="$(container_state prediction-api)"

    case "$state" in
      running|healthy)
        return 0
        ;;
      exited|dead)
        show_service_logs prediction-api
        die "The prediction API container exited during startup."
        ;;
      *)
        sleep 2
        ;;
    esac
  done

  show_service_logs prediction-api
  die "Timed out waiting for the prediction API container to start."
}

bootstrap_prediction_model() {
  wait_for_prediction_api_container

  if prediction_api_ready; then
    return 0
  fi

  log "Prediction model artifacts are missing or not loaded. Training them now."
  compose run --rm model-trainer

  log "Restarting prediction-api so it reloads the newly trained artifacts."
  compose restart prediction-api >/dev/null
  wait_for_prediction_api_ready

  if container_is_running dashboard; then
    log "Restarting dashboard to clear any cached prediction error state."
    compose restart dashboard >/dev/null
  fi
}

print_summary() {
  log "Project is up."
  compose ps

  cat <<'EOF'

Useful URLs:
  API:             http://localhost:8000/
  Prediction API:  http://localhost:8001/
  Grafana:         http://localhost:3000/
  Redpanda UI:     http://localhost:8080/
  Mongo Express:   http://localhost:8082/
  Dashboard:       http://localhost:8501/
EOF
}

cleanup_logs() {
  if [ -n "$LAST_UP_LOG" ] && [ -f "$LAST_UP_LOG" ]; then
    rm -f "$LAST_UP_LOG"
  fi
}

main() {
  local wait_rc
  local image_pruned_for_retry=0

  trap cleanup_logs EXIT

  while [ "$#" -gt 0 ]; do
    case "$1" in
      --build)
        BUILD_IMAGES=1
        ;;
      --prune-images)
        PRUNE_IMAGES=1
        ;;
      --fresh-data)
        FRESH_DATA=1
        ;;
      --timeout)
        shift
        [ "$#" -gt 0 ] || die "--timeout requires a value."
        STARTUP_TIMEOUT="$1"
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "Unknown option: $1"
        ;;
    esac
    shift
  done

  ensure_prerequisites
  prune_build_cache

  if [ "$PRUNE_IMAGES" -eq 1 ]; then
    prune_unused_images
  fi

  while true; do
    stop_existing_stack

    if [ "$FRESH_DATA" -eq 1 ]; then
      reset_state_volumes
      FRESH_DATA=0
    fi

    if ! run_compose_up; then
      if startup_hit_disk_pressure && [ "$image_pruned_for_retry" -eq 0 ]; then
        warn "Docker storage still looks tight. Retrying once after pruning unused images."
        prune_unused_images
        image_pruned_for_retry=1
        continue
      fi
      die "docker compose up failed. Check the logs above."
    fi

    set +e
    wait_for_stack
    wait_rc=$?
    set -e

    if [ "$wait_rc" -eq 0 ]; then
      break
    fi

    if [ "$wait_rc" -eq 20 ] && [ "$image_pruned_for_retry" -eq 0 ]; then
      warn "A container reported 'No space left on device'. Retrying once after pruning unused images."
      prune_unused_images
      image_pruned_for_retry=1
      continue
    fi

    die "The Docker stack did not become healthy."
  done

  bootstrap_prediction_model
  print_summary
}

main "$@"
