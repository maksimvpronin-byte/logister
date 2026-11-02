#!/bin/sh
OUT="${LOG_FILE:-/logs/app.log}"
HOST_NAME="${LOG_HOST:-app-01}"
INTERVAL_MS="${INTERVAL_MS:-700}"

mkdir -p "$(dirname "$OUT")"
: > "$OUT"

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
  stream="all"

  case "$level" in
    warn) status=429 ;;
    error) status=500 ;;
  esac

  case "$source" in
    nginx)
      msg="$client - $user [$(date -u '+%d/%b/%Y:%H:%M:%S +0000')] \"GET $path HTTP/1.1\" $status $(( $(rand 900) + 40 ))"
      stream="web-access"
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
      stream="system"
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

  printf '{"timestamp":"%s","message":"%s","source":"%s","level":"%s","host":"%s","stream":"%s","fields":{"seq":%s,"path":"%s","client":"%s"}}\n' \
    "$ts" "$msg_esc" "$source" "$level" "$HOST_NAME" "$stream" "$i" "$path" "$client" >> "$OUT"

  if [ $((i % 500)) -eq 0 ]; then
    tail -n 200 "$OUT" > "$OUT.tmp" && mv "$OUT.tmp" "$OUT"
  fi

  if command -v usleep >/dev/null 2>&1; then
    usleep $((INTERVAL_MS * 1000))
  else
    sleep 1
  fi
done
