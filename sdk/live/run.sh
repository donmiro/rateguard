#!/usr/bin/env bash
# Runs each client's live tests against a real rateguard-sidecar, started
# with a limit of 2 and a burst of 2 on free local ports, and stops it. Fails
# if any live test fails, or is skipped: a green run must have talked to the
# sidecar. With --admin: the clients' admin tests change limits, and put
# each back as it was.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"

cargo build --locked -p rateguard-sidecar
sidecar="${CARGO_TARGET_DIR:-$root/target}/debug/rateguard-sidecar"

# A free TCP port for the API and a free UDP port for gossip. Another process
# could take one between now and the sidecar's bind, which the health wait
# below turns into a loud failure.
free_port() {
  python3 - "$1" <<'PY'
import socket, sys
kind = socket.SOCK_DGRAM if sys.argv[1] == "udp" else socket.SOCK_STREAM
s = socket.socket(socket.AF_INET, kind)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
PY
}
http_port="$(free_port tcp)"
gossip_port="$(free_port udp)"

log="$(mktemp)"
"$sidecar" --limit 2 --burst 2 --admin \
  --listen "127.0.0.1:$http_port" --bind "127.0.0.1:$gossip_port" >"$log" 2>&1 &
pid=$!
cleanup() {
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  rm -f "$log"
}
trap cleanup EXIT

url="http://127.0.0.1:$http_port"
for _ in $(seq 1 100); do
  if curl -fsS "$url/healthz" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "the sidecar exited:" >&2
    cat "$log" >&2
    exit 1
  fi
  sleep 0.1
done
curl -fsS "$url/healthz" >/dev/null || { echo "the sidecar never became healthy" >&2; cat "$log" >&2; exit 1; }

export RATEGUARD_LIVE_URL="$url"

# A skipped live test counts as a failure: each runner's output is checked
# for the tests that must have run, and for no skips.
out="$(mktemp)"
trap 'cleanup; rm -f "$out"' EXIT

run() {
  local name="$1" passed="$2" skipped="$3"
  shift 3
  echo "== $name"
  "$@" 2>&1 | tee "$out"
  # pipefail makes a failing test fail the script here, through set -e.
  grep -Eq "$passed" "$out" || { echo "$name: no live test ran" >&2; exit 1; }
  if grep -Eq "$skipped" "$out"; then
    echo "$name: a live test was skipped" >&2
    exit 1
  fi
}

(cd sdk/python && run python 'Ran [1-9][0-9]* test' 'skipped' \
  env PYTHONPATH=src python3 -W error::ResourceWarning -m unittest discover -s tests -k Live -v)
(cd sdk/typescript && run typescript '^# pass [1-9]' '^# skipped [1-9]' \
  node --test test/live.test.js)
(cd sdk/go && run go '^--- PASS' '^--- SKIP' \
  go test -count=1 -run Live -v ./...)
echo "live tests passed"
