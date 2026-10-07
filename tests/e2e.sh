#!/usr/bin/env bash
set -euo pipefail
# End-to-end test: two alink identities on this machine talking over iroh in local mode.
# Usage: cargo build && tests/e2e.sh   (requires jq)
BIN=${BIN:-"$(cd "$(dirname "$0")/.." && pwd)/target/debug/alink"}
ROOT=$(mktemp -d)
A() { ALINK_HOME="$ROOT/alice" "$BIN" "$@"; }
D() { ALINK_HOME="$ROOT/david" "$BIN" "$@"; }
A init --name alice-claude >/dev/null
D init --name david-codex >/dev/null
for h in alice david; do sed -i.bak 's/mode = "n0"/mode = "local"/' "$ROOT/$h/config.toml"; done
cat >> "$ROOT/alice/config.toml" <<'TOML'

[handler.review]
description = "Fake reviewer for tests"
command = ["sh", "-c", "printf 'REVIEWED by %s for %s: ' \"$ALINK_HANDLER\" \"$ALINK_PEER\"; grep -c '' | tr -d ' '; echo lines"]
timeout = "30s"
peers = ["david-codex"]

[handler.secret]
command = ["true"]
peers = ["someone-else"]
TOML

echo "== alice serve"
ALINK_HOME="$ROOT/alice" "$BIN" serve 2>"$ROOT/alice-serve.log" & APID=$!
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$ROOT"' EXIT
sleep 1
A whoami --json | jq -c '{name, node_running}'

echo "== invite (via control socket) + join (temp node)"
TICKET=$(A invite --no-wait --json | jq -r .invite)
echo "${TICKET:0:40}..."
D join "$TICKET" --yes --json | jq -c '{name, handlers: [.card.handlers[].name]}'
A peers --json | jq -c '[.[] | {name, handlers: [.card.handlers[].name]}]'
echo "reuse invite:"; D join "$TICKET" --yes --json 2>&1 | tail -1 || true

echo "== run request with stdin attachment"
REQ=$(printf 'a\nb\nc\n' | D run alice-claude review "please review" --stdin --json)
echo "$REQ" | jq -c '{delivery, state}'
REQ_ID=$(echo "$REQ" | jq -r .request_id)
D wait "$REQ_ID" --timeout 30s --json | jq -c '{state, response}'

echo "== run --wait"
D run alice-claude review "inline" --wait --timeout 30s --json | jq -c '{state, response}'

echo "== forbidden handler"
set +e; D run alice-claude secret "x" --wait --timeout 30s --json 2>/dev/null | jq -c '{state, note}'; echo "exit=${PIPESTATUS[0]}"; set -e

echo "== messages + reply"
SENT=$(D send alice-claude "hello from codex" --json); echo "$SENT" | jq -c '{delivery}'
THREAD=$(echo "$SENT" | jq -r .thread_id)
MSG=$(A inbox --json | jq -r '.[0].id'); A inbox --all --json | jq -c '[.[] | {from, text}]'
A reply "$MSG" "hi david" --json | jq -c '{delivery}'
D wait "$THREAD" --timeout 30s --json | jq -c '[.[] | {from, text, reply_to: (.reply_to != null)}]'

echo "== scoped wait reports unread messages in other threads"
D send alice-claude "new topic in a new thread" --json >/dev/null
set +e; A wait "$THREAD" --timeout 1s --json | jq -c '{status, other_unread}'; set -e
A inbox --json | jq -c '[.[] | .text]'

echo "== listen notifies once per batch, retries a failing notify, keeps messages unread"
touch "$ROOT/fail-once"
ALINK_HOME="$ROOT/alice" "$BIN" listen --json --notify "if [ -e '$ROOT/fail-once' ]; then rm '$ROOT/fail-once'; exit 1; fi; echo \"\$ALINK_COUNT from \$ALINK_FROM\" >> '$ROOT/notified'" > "$ROOT/listen.out" 2>"$ROOT/listen.err" & LPID=$!
sleep 1
D send alice-claude "first" --json >/dev/null
for _ in $(seq 1 40); do [ -s "$ROOT/notified" ] && break; sleep 0.25; done
cat "$ROOT/notified"
grep -c 'retrying' "$ROOT/listen.err"
D send alice-claude "second" --json >/dev/null
for _ in $(seq 1 40); do [ "$(wc -l < "$ROOT/notified")" -ge 2 ] && break; sleep 0.25; done
kill $LPID; wait $LPID 2>/dev/null || true
cat "$ROOT/notified"
jq -c '{count, from}' "$ROOT/listen.out"
A inbox --json | jq -c '[.[] | .text]'

echo "== offline: alice down, david queues"
kill $APID; wait $APID 2>/dev/null || true
REQ2=$(D run alice-claude review "while offline" --json 2>/dev/null); echo "$REQ2" | jq -c '{delivery}'
REQ2_ID=$(echo "$REQ2" | jq -r .request_id)
D outbox --json | jq -c '[.[] | {kind, delivery, attempts}]'
echo "== alice back up; david waits (temp node delivers + receives)"
ALINK_HOME="$ROOT/alice" "$BIN" serve 2>>"$ROOT/alice-serve.log" & APID2=$!
sleep 1
D wait "$REQ2_ID" --timeout 60s --json | jq -c '{state, response}'
D requests --json | jq -c '[.[] | {handler, state}]'
A requests --incoming --json | jq -c '[.[] | {peer, handler, state}]'

echo "== david serve: alice offline, david's daemon delivers when alice returns"
ALINK_HOME="$ROOT/david" "$BIN" serve 2>"$ROOT/david-serve.log" & DPID=$!
kill $APID2; wait $APID2 2>/dev/null || true
sleep 1
REQ3=$(D run alice-claude review "daemon queued" --json 2>/dev/null); echo "$REQ3" | jq -c '{delivery}'
REQ3_ID=$(echo "$REQ3" | jq -r .request_id)
ALINK_HOME="$ROOT/alice" "$BIN" serve 2>>"$ROOT/alice-serve.log" & APID3=$!
D wait "$REQ3_ID" --timeout 60s --json | jq -c '{state, response}'
kill $DPID $APID3; wait 2>/dev/null || true
echo "== ok"
