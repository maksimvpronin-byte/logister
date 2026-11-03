#!/bin/sh
OUT="${LOG_FILE:-/logs/app.log}"
HOST_NAME="${LOG_HOST:-app-01}"
INTERVAL_MS="${INTERVAL_MS:-700}"
ROTATE_EVERY="${ROTATE_EVERY:-500}"
MAX_MESSAGES="${MAX_MESSAGES:-0}"
RUN_ID=$(cat /proc/sys/kernel/random/uuid)

mkdir -p "$(dirname "$OUT")"
touch "$OUT"

rand() {
  max="$1"
  echo $(( $(od -An -N2 -tu2 /dev/urandom | tr -d ' ') % max ))
}

pick() {
  set -- $1
  n=$#
  idx=$(rand "$n")
  idx=$((idx + 1))
  i=1
  for item in "$@"; do
    if [ "$i" -eq "$idx" ]; then
      echo "$item"
      return
    fi
    i=$((i + 1))
  done
}

echo "loggen writing to $OUT every ${INTERVAL_MS}ms" >&2

i=0
while true; do
  i=$((i + 1))
  level=$(pick "info info info warn warn error")
  source=$(pick "nginx auth-service billing api gateway cron")
  path=$(pick "/api/health /api/search /login /checkout /metrics /admin")
  user=$(pick "alice bob carol dave eve")
  client=$(pick "10.0.0.12 10.0.0.55 192.168.1.10 203.0.113.8")
  status=200

  case "$level" in
    warn) status=429 ;;
    error) status=500 ;;
  esac

  case "$source" in
    nginx)
      msg="$client - $user [$(date -u '+%d/%b/%Y:%H:%M:%S +0000')] \"GET $path HTTP/1.1\" $status $(( $(rand 900) + 40 ))"
      ;;
    auth-service)
      if [ "$level" = "error" ] || [ "$level" = "warn" ]; then
        msg="login failed for user $user from $client"
      else
        msg="login success for user $user from $client"
      fi
      ;;
    billing)
      msg="payment processed order=ORD-$i amount=$(( $(rand 200) + 1 )).$(( $(rand 90) + 10 )) currency=USD"
      ;;
    cron)
      msg="cron job backup-step-$((i % 5)) finished in $(( $(rand 40) + 1 ))s"
      level="info"
      ;;
    gateway)
      msg="upstream $path responded status=$status latency_ms=$(( $(rand 400) + 5 ))"
      ;;
    *)
      msg="request id=req-$i path=$path status=$status"
      ;;
  esac

  msg_esc=$(printf '%s' "$msg" | sed 's/\\/\\\\/g; s/"/\\"/g')
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)

  # The API applies the editable stream rules; the source only describes events.
  line=$(printf '{"id":"%s-%s","timestamp":"%s","message":"%s","source":"%s","level":"%s","host":"%s","fields":{"seq":%s,"path":"%s","client":"%s"}}' \
    "$RUN_ID" "$i" "$ts" "$msg_esc" "$source" "$level" "$HOST_NAME" "$i" "$path" "$client")
  # On ENOSPC a write can be partial. Restore the last complete file and move it
  # aside before retrying this same event, so tail never joins partial JSON lines.
  while true; do
    size=$(wc -c < "$OUT")
    if printf '%s\n' "$line" >> "$OUT"; then break; fi
    truncate -s "$size" "$OUT"
    mv "$OUT" "$OUT.$RUN_ID.retry-$i" 2>/dev/null || true
    sleep 1
    touch "$OUT"
  done

  if [ "$ROTATE_EVERY" -gt 0 ] && [ $((i % ROTATE_EVERY)) -eq 0 ]; then
    mv "$OUT" "$OUT.$RUN_ID.$i"
    touch "$OUT"
  fi

  if [ "$MAX_MESSAGES" -gt 0 ] && [ "$i" -ge "$MAX_MESSAGES" ]; then exit 0; fi

  if command -v usleep >/dev/null 2>&1; then
    usleep $((INTERVAL_MS * 1000))
  else
    sleep 1
  fi
done
