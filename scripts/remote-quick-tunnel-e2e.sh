#!/usr/bin/env bash
set -euo pipefail

# Env-gated real Cloudflare Quick Tunnel smoke.
#
# Quick Tunnels are the official zero-config Cloudflare development/testing path:
#   cloudflared tunnel --url http://localhost:8080
# They are intentionally not a production acceptance substitute. This harness
# starts a temporary lucarned daemon, opens a free trycloudflare.com tunnel via
# `lucarned remote start`, validates auth/read-only isolation through the public
# URL, then stops the gateway/tunnel and tears down the daemon.

if [[ "${LUCARNE_QUICK_TUNNEL_E2E:-}" != "1" ]]; then
  echo "skip: set LUCARNE_QUICK_TUNNEL_E2E=1 to run the real Quick Tunnel E2E"
  exit 0
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${LUCARNED_BIN:-$ROOT/target/debug/lucarned}"
if [[ ! -x "$BIN" ]]; then
  echo "error: lucarned binary not found at $BIN; run cargo +nightly build -Zbuild-dir-new-layout -p lucarned" >&2
  exit 1
fi

for tool in curl jq node cloudflared; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: required tool not found: $tool" >&2
    exit 1
  fi
done

if ! command -v rmux >/dev/null 2>&1 && [[ ! -x "${HOME:-}/.cargo/bin/rmux" ]]; then
  echo "error: rmux not found on PATH or ~/.cargo/bin/rmux" >&2
  exit 1
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/lucarne-quick-e2e.XXXXXX")"
DAEMON_PID=""
public_url=""
host=""
GATEWAY_PORT="${LUCARNE_QUICK_TUNNEL_GATEWAY_PORT:-19092}"
CONTROL_PORT="${LUCARNE_QUICK_TUNNEL_CONTROL_PORT:-19093}"
FULL_TOKEN="${LUCARNE_QUICK_TUNNEL_FULL_TOKEN:-full-0123456789abcdef0123456789abcdef}"
READONLY_TOKEN="${LUCARNE_QUICK_TUNNEL_READONLY_TOKEN:-readonly-0123456789abcdef0123456789}"
READY_RETRIES="${LUCARNE_QUICK_TUNNEL_READY_RETRIES:-120}"

cleanup() {
  local status=$?
  set +e
  if [[ "$status" -ne 0 ]]; then
    echo "diagnostics: Quick Tunnel E2E failed; workdir was $WORK" >&2
    if [[ -n "${public_url:-}" ]]; then
      echo "diagnostics: public_url=$public_url" >&2
      if [[ -n "${host:-}" ]]; then
        echo "diagnostics: host=$host" >&2
        if command -v dig >/dev/null 2>&1; then
          echo "diagnostics: dig +short $host" >&2
          dig +time=2 +tries=1 +short "$host" >&2 || true
        fi
        if command -v dscacheutil >/dev/null 2>&1; then
          echo "diagnostics: dscacheutil -q host -a name $host" >&2
          dscacheutil -q host -a name "$host" >&2 || true
        fi
      fi
    fi
    if [[ -f "$WORK/curl.err" && -s "$WORK/curl.err" ]]; then
      echo "diagnostics: last curl stderr:" >&2
      tail -n 20 "$WORK/curl.err" >&2 || true
    fi
    if [[ -f "$WORK/daemon.log" ]]; then
      echo "diagnostics: daemon log tail:" >&2
      tail -n 120 "$WORK/daemon.log" >&2 || true
    fi
    if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
      echo "diagnostics: remote status before cleanup:" >&2
      "$BIN" remote status --control-port "$CONTROL_PORT" --json >&2 || true
    fi
  fi
  if [[ -n "$DAEMON_PID" ]] && kill -0 "$DAEMON_PID" >/dev/null 2>&1; then
    "$BIN" remote stop --control-port "$CONTROL_PORT" --json >/dev/null 2>&1 || true
    kill -INT "$DAEMON_PID" >/dev/null 2>&1 || true
    for _ in $(seq 1 40); do
      kill -0 "$DAEMON_PID" >/dev/null 2>&1 || break
      sleep 0.25
    done
    kill "$DAEMON_PID" >/dev/null 2>&1 || true
  fi
  if [[ "$status" -ne 0 && "${LUCARNE_QUICK_TUNNEL_KEEP_FAILURE:-}" == "1" ]]; then
    echo "diagnostics: preserving failed workdir because LUCARNE_QUICK_TUNNEL_KEEP_FAILURE=1: $WORK" >&2
  else
    rm -rf "$WORK"
  fi
  return "$status"
}
trap cleanup EXIT

mkdir -p "$WORK/home" "$WORK/xdg-config" "$WORK/xdg-cache"
export HOME="$WORK/home"
export XDG_CONFIG_HOME="$WORK/xdg-config"
export XDG_CACHE_HOME="$WORK/xdg-cache"

cat >"$WORK/lucarned.yaml" <<YAML
agents: []
state:
  db: "$WORK/state.sqlite3"
logging:
  stderr_filter: warn
  dir: "$WORK/logs"
health:
  enabled: false
remote:
  enabled: false
  provider: cloudflared
  gateway_addr: 127.0.0.1:$GATEWAY_PORT
  control_addr: 127.0.0.1:$CONTROL_PORT
  auth_token: "$FULL_TOKEN"
  readonly_token: "$READONLY_TOKEN"
  insecure: false
  providers:
    cloudflared:
      token: ""
      public_url: ""
      binary_path: "$(command -v cloudflared)"
channels:
  telegram:
    enabled: false
  wechat:
    enabled: false
YAML

export LUCARNE_CONFIG="$WORK/lucarned.yaml"
export LUCARNED_REMOTE_CONTROL_ADDR="127.0.0.1:$CONTROL_PORT"
export LUCARNED_REMOTE_GATEWAY_ADDR="127.0.0.1:$GATEWAY_PORT"

"$BIN" >"$WORK/daemon.log" 2>&1 &
DAEMON_PID="$!"

for _ in $(seq 1 80); do
  if "$BIN" remote status --control-port "$CONTROL_PORT" --json >/dev/null 2>"$WORK/status.err"; then
    break
  fi
  sleep 0.25
done

status_json="$("$BIN" remote status --control-port "$CONTROL_PORT" --json)"
if [[ "$(jq -r '.running' <<<"$status_json")" != "false" ]]; then
  echo "error: expected cold daemon remote status running=false, got: $status_json" >&2
  exit 1
fi

start_json="$("$BIN" remote start --control-port "$CONTROL_PORT" --json)"
public_url="$(jq -r '.public_url // empty' <<<"$start_json")"
if [[ -z "$public_url" || "$public_url" != https://*.trycloudflare.com/* ]]; then
  echo "error: expected trycloudflare public URL, got: $start_json" >&2
  exit 1
fi
public_url="${public_url%/}"
host="$(node -e 'console.log(new URL(process.argv[1]).host)' "$public_url")"

curl_cf() {
  : >"$WORK/resp.body"
  : >"$WORK/curl.err"
  curl -sS -o "$WORK/resp.body" -w "%{http_code}" \
    --connect-timeout 10 \
    --max-time 30 \
    2>"$WORK/curl.err" \
    "$@"
}

wait_for_http_status() {
  local expected="$1"
  shift
  local code=""
  for _ in $(seq 1 "$READY_RETRIES"); do
    code="$(curl_cf "$@" || true)"
    if [[ "$code" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "error: expected HTTP $expected from $*, got $code" >&2
  if [[ -s "$WORK/curl.err" ]]; then
    cat "$WORK/curl.err" >&2 || true
  fi
  cat "$WORK/resp.body" >&2 || true
  return 1
}

wait_for_http_status 401 "$public_url/api/sessions"

code="$(curl_cf "$public_url/api/remote/status" || true)"
if [[ "$code" == "200" ]]; then
  echo "error: public gateway exposed /api/remote/status" >&2
  cat "$WORK/resp.body" >&2 || true
  exit 1
fi

wait_for_http_status 200 -H "Authorization: Bearer $FULL_TOKEN" "$public_url/api/sessions"

wait_for_http_status 403 -X POST -H "Authorization: Bearer $READONLY_TOKEN" -H "content-type: application/json" \
  --data '{"title":"should-not-create"}' "$public_url/api/sessions"

wait_for_http_status 200 -X POST -H "Authorization: Bearer $FULL_TOKEN" "$public_url/auth/ticket"
ticket_json="$(cat "$WORK/resp.body")"
ticket="$(jq -r '.ticket' <<<"$ticket_json")"
wait_for_http_status 200 -X POST -H "Authorization: Bearer $READONLY_TOKEN" "$public_url/auth/ticket"
ro_ticket_json="$(cat "$WORK/resp.body")"
ro_ticket="$(jq -r '.ticket' <<<"$ro_ticket_json")"

HOST="$host" TICKET="$ticket" RO_TICKET="$ro_ticket" node <<'NODE'
const tls = require("tls");
const crypto = require("crypto");

function ws(path, ticket, sendFrame) {
  return new Promise((resolve, reject) => {
    const key = crypto.randomBytes(16).toString("base64");
    const socket = tls.connect({
      host: process.env.HOST,
      port: 443,
      servername: process.env.HOST,
      ALPNProtocols: ["http/1.1"],
    });
    let raw = Buffer.alloc(0);
    let upgraded = false;
    let done = false;
    const timer = setTimeout(() => {
      if (!done) reject(new Error("websocket timeout"));
      socket.destroy();
    }, 15000);
    socket.on("secureConnect", () => {
      socket.write(
        `GET ${path}?ticket=${encodeURIComponent(ticket)} HTTP/1.1\r\n` +
          `Host: ${process.env.HOST}\r\n` +
          `Upgrade: websocket\r\nConnection: Upgrade\r\n` +
          `Sec-WebSocket-Key: ${key}\r\nSec-WebSocket-Version: 13\r\n\r\n`
      );
    });
    socket.on("data", (chunk) => {
      raw = Buffer.concat([raw, chunk]);
      if (!upgraded) {
        const sep = raw.indexOf("\r\n\r\n");
        if (sep === -1) return;
        const head = raw.slice(0, sep).toString();
        if (!head.startsWith("HTTP/1.1 101")) {
          done = true;
          clearTimeout(timer);
          socket.destroy();
          reject(new Error(`upgrade failed: ${head.split("\r\n")[0]}`));
          return;
        }
        upgraded = true;
        raw = raw.slice(sep + 4);
        if (sendFrame) socket.write(encodeFrame(JSON.stringify(sendFrame)));
      }
      while (raw.length >= 2) {
        const b1 = raw[0], b2 = raw[1];
        let len = b2 & 0x7f, offset = 2;
        if (len === 126) {
          if (raw.length < 4) return;
          len = raw.readUInt16BE(2); offset = 4;
        } else if (len === 127) {
          if (raw.length < 10) return;
          len = Number(raw.readBigUInt64BE(2)); offset = 10;
        }
        if (raw.length < offset + len) return;
        const payload = raw.slice(offset, offset + len).toString();
        raw = raw.slice(offset + len);
        if ((b1 & 0x0f) === 1) {
          const msg = JSON.parse(payload);
          done = true;
          clearTimeout(timer);
          socket.end();
          resolve(msg);
          return;
        }
      }
    });
    socket.on("error", reject);
  });
}

function encodeFrame(text) {
  const payload = Buffer.from(text);
  const mask = crypto.randomBytes(4);
  const head = payload.length < 126
    ? Buffer.from([0x81, 0x80 | payload.length])
    : Buffer.from([0x81, 0x80 | 126, payload.length >> 8, payload.length & 0xff]);
  const masked = Buffer.alloc(payload.length);
  for (let i = 0; i < payload.length; i++) masked[i] = payload[i] ^ mask[i % 4];
  return Buffer.concat([head, mask, masked]);
}

(async () => {
  const first = await ws("/ws", process.env.TICKET);
  if (first.type !== "session_list") throw new Error(`expected session_list, got ${JSON.stringify(first)}`);
  const refused = await ws("/ws", process.env.RO_TICKET, { type: "create_session", title: "blocked" });
  if (refused.type !== "error" || refused.code !== 403) {
    throw new Error(`expected readonly create refusal, got ${JSON.stringify(refused)}`);
  }
})().catch((err) => {
  console.error(err.stack || err.message);
  process.exit(1);
});
NODE

stop_json="$("$BIN" remote stop --control-port "$CONTROL_PORT" --json)"
if [[ "$(jq -r '.running' <<<"$stop_json")" != "false" ]]; then
  echo "error: expected stop running=false, got: $stop_json" >&2
  exit 1
fi

echo "ok: Quick Tunnel E2E passed for $public_url"
