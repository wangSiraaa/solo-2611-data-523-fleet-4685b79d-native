#!/usr/bin/env bash
# Cross-process acceptance demo for materials/534.
#
# A real shard read on A is paused by a fixture (in-process, tests.py), which a
# shell cannot do.  This script instead demonstrates the sequential protocol the
# CLI exposes: a maintenance request drains A with no work, the next step really
# reads B, the receipt is closed, and verify re-reads the raw CSV independently.
set -euo pipefail
cd "$(dirname "$0")"
PY=${PY:-python3.11}
ROOT=${1:-$(mktemp -d)}

echo "== init =="
$PY cli.py "$ROOT" init --shards 3 --rows 50 >/dev/null

echo "== complete shard-1 on A =="
$PY cli.py "$ROOT" step >/dev/null
$PY cli.py "$ROOT" source-status | $PY -c 'import sys,json;d=json.load(sys.stdin);print({k:v["read_count"] for k,v in d.items()})'

echo "== request maintenance =="
$PY cli.py "$ROOT" maintain req-1 | $PY -c 'import sys,json;d=json.load(sys.stdin);print("status",d["status"],"close",d["close"])'
$PY cli.py "$ROOT" source-status | $PY -c 'import sys,json;d=json.load(sys.stdin);print("A",d["A"]["state"],"closed",d["A"]["fixture_closed"])'

echo "== next step must read B =="
$PY cli.py "$ROOT" step | $PY -c 'import sys,json;d=json.load(sys.stdin);print("shard",d["shard_id"],"source",d["source"],"status",d["status"])'
for _ in 1 2 3 4 5; do $PY cli.py "$ROOT" step >/dev/null; done

echo "== status =="
$PY cli.py "$ROOT" batch-status | $PY -c 'import sys,json;d=json.load(sys.stdin);print(d["state"],"rows",d["rows"],{k:v["source"] for k,v in d["shards"].items()})'

echo "== verify (independent raw CSV re-read) =="
$PY cli.py "$ROOT" verify | $PY -c 'import sys,json;d=json.load(sys.stdin);print("ok",d["ok"],"origin",d["origin_total"],"db",d["db_total"],"incomplete",d["incomplete"])'

echo "== source call log =="
cat "$ROOT/calls.log"
