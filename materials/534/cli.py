#!/usr/bin/env python3.11
"""Command-line interface for the batch collection service.

The workspace layout is deliberately simple and re-derivable on every command:

    <root>/descriptor.json                 frozen descriptor (never rewritten)
    <root>/sources/A/<shard>.csv           real primary files
    <root>/sources/B/<shard>.csv           real backup files
    <root>/ingest.db                       SQLite row/shard state
    <root>/state.json                      sequential-run state
    <root>/calls.log                       independent source call log

Usage:
    python3.11 cli.py <root> start-batch
    python3.11 cli.py <root> step
    python3.11 cli.py <root> maintain [request_id]
    python3.11 cli.py <root> cancel-maintenance [request_id]
    python3.11 cli.py <root> cancel-batch
    python3.11 cli.py <root> source-status
    python3.11 cli.py <root> batch-status
    python3.11 cli.py <root> verify
    python3.11 cli.py <root> init --shards N [--rows M] [--divergent shard-2,...]
"""

from __future__ import annotations

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from descriptor import load_descriptor, save_descriptor  # noqa: E402
from harness import build_workspace, CompositeSource  # noqa: E402
from service import Service  # noqa: E402


def _paths(root):
    return os.path.join(root, "descriptor.json"), os.path.join(root, "state.json")


def load_service(root):
    dpath, spath = _paths(root)
    desc = load_descriptor(dpath)
    sources = {}
    snapshots = {}
    for spec_source in [desc.get("primary", "A")] + list(desc.get("backups", [])):
        files = {}
        for spec in desc["shards"]:
            files[spec["shard_id"]] = os.path.join(root, "sources", spec_source, os.path.basename(spec["origin"]))
        sources[spec_source] = CompositeSource(spec_source, files, snapshot=desc["snapshot"])
    svc = Service(root, sources, desc)
    if os.path.exists(spath):
        with open(spath, "r", encoding="utf-8") as fh:
            svc.restore_state(json.load(fh))
    return svc


def save_service(root, svc):
    dpath, spath = _paths(root)
    save_descriptor(svc.descriptor, dpath)
    with open(spath, "w", encoding="utf-8") as fh:
        json.dump(svc.export_state(), fh, indent=2, sort_keys=True)
        fh.write("\n")


def append_calls(root, svc):
    """Keep an independent, cross-invocation source call log (append-only).

    Each process rebuilds fresh fixtures, so their audit holds exactly the real
    reads/closes of this invocation; append them all.
    """
    path = os.path.join(root, "calls.log")
    entries = []
    for name in svc.registry._sources:
        entries.extend({"source": name, **entry} for entry in svc.registry._sources[name].audit)
    with open(path, "a", encoding="utf-8") as fh:
        for entry in entries:
            fh.write(json.dumps(entry, sort_keys=True) + "\n")


def cmd_init(root, args):
    shards = int(args["--shards"])
    rows = int(args.get("--rows", 50))
    divergent = [s for s in args.get("--divergent", "").split(",") if s]
    overrides = {sid: [("bad", 0)] for sid in divergent}
    ws = build_workspace(root, num_shards=shards, rows_per_shard=rows, b_overrides=overrides)
    # Persist the real source trees so later commands rebuild identical files.
    svc = Service(root, {"A": ws["A"], "B": ws["B"]}, ws["descriptor"])
    save_service(root, svc)
    return {"descriptor": os.path.join(root, "descriptor.json"), "shards": shards, "rows": rows}


def main(argv):
    if len(argv) < 3:
        print(__doc__)
        return 2
    root = argv[1]
    command = argv[2]
    extra = argv[3:]
    args = {}
    i = 0
    while i < len(extra):
        if extra[i].startswith("--"):
            args[extra[i]] = extra[i + 1] if i + 1 < len(extra) else ""
            i += 2
        else:
            args.setdefault("_pos", []).append(extra[i])
            i += 1

    if command == "init":
        print(json.dumps(cmd_init(root, args), indent=2))
        return 0

    svc = load_service(root)
    if command == "start-batch":
        result = svc.start_batch()
    elif command == "step":
        result = svc.step()
    elif command == "maintain":
        rid = args["_pos"][0] if args.get("_pos") else None
        result = svc.maintain(rid)
    elif command == "cancel-maintenance":
        rid = args["_pos"][0] if args.get("_pos") else None
        result = svc.cancel_maintenance(rid)
    elif command == "cancel-batch":
        result = svc.cancel_batch()
    elif command == "source-status":
        result = svc.source_status()
    elif command == "batch-status":
        result = svc.batch_status()
    elif command == "verify":
        result = svc.verify()
    else:
        print(f"unknown command: {command}", file=sys.stderr)
        return 2
    save_service(root, svc)
    append_calls(root, svc)
    print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
