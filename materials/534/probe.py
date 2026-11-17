#!/usr/bin/env python3.11
"""Reproduce the original local defect and show it is fixed.

This is a lightweight demonstration, not the full acceptance suite.  The
authoritative checks live in ``tests.py``.
"""

import json
import os
import tempfile
import threading

from controlled_source import ControlledSource
from harness import build_workspace
from service import Service


def fixture_probe():
    """The real fixture still refuses to close while a read is active."""
    src = ControlledSource("A", "shard-1.csv")
    src.allow_read.clear()
    received = []
    th = threading.Thread(target=lambda: received.append(src.read()))
    th.start()
    assert src.before_read.wait(2)
    try:
        src.close()
        held = None
    except RuntimeError as exc:
        held = str(exc)
    src.allow_read.set()
    th.join(2)
    src.close()
    out = {"held_read_close": held, "received_bytes": len(received[0]), "closed": src.closed, "audit": src.audit}

    src2 = ControlledSource("A", "shard-2.csv")
    src2.allow_callback.clear()
    received2 = []
    th2 = threading.Thread(target=lambda: received2.append(src2.read()))
    th2.start()
    assert src2.before_callback.wait(2)
    try:
        src2.close()
        cb = None
    except RuntimeError as exc:
        cb = str(exc)
    src2.allow_callback.set()
    th2.join(2)
    src2.close()
    out["callback_hold_close"] = cb
    out["callback_hold_closed"] = src2.closed
    return out


def maintenance_probe():
    """Acquire on A, pause a real read, request maintenance, dispatch shard-2.

    shard-2 must really read B while A is DRAINING; A may only close after the
    paused shard-1 read is released.  shard-1 imports exactly once.
    """
    with tempfile.TemporaryDirectory() as td:
        ws = build_workspace(td, num_shards=2, rows_per_shard=50)
        A, B = ws["A"], ws["B"]
        f1 = A.fixture("shard-1")
        f1.allow_read.clear()
        svc = Service(td, {"A": A, "B": B}, ws["descriptor"])
        t1 = svc.ingestion.dispatch("batch-534", "shard-1")
        assert f1.before_read.wait(3)
        maint = svc.maintain("probe-maint")
        out2 = svc.ingestion._run("batch-534", "shard-2", deliver=True, block=True)
        during = {
            "ticket_held_on_A": maint["in_flight"],
            "a_state_during": svc.source_status()["A"]["state"],
            "a_closed_during": A.closed,
            "shard2_source": out2.get("source"),
            "shard2_status": out2.get("status"),
        }
        f1.allow_read.set()
        t1.join(5)
        rows = svc.ingestion.db.execute(
            "SELECT shard_id, COUNT(*) FROM rows GROUP BY shard_id ORDER BY shard_id"
        ).fetchall()
        after = {
            "a_state_final": svc.source_status()["A"]["state"],
            "a_closed_final": A.closed,
            "receipt_status": svc.source_status()["A"]["maintenance"]["status"],
            "shard1_state": svc.batch_status()["shards"]["shard-1"]["state"],
            "row_counts": dict(rows),
            "row_sources": dict(svc.ingestion.db.execute("SELECT shard_id,source FROM shards_done").fetchall()),
        }
        return {"during": during, "after": after, "verify_ok": svc.verify()["ok"]}


def divergent_backup_probe():
    """B's file name matches but its digest does not: shard stays UNAVAILABLE."""
    with tempfile.TemporaryDirectory() as td:
        ws = build_workspace(td, num_shards=1, b_overrides={"shard-1": [("x", 0)]})
        svc = Service(td, {"A": ws["A"], "B": ws["B"]}, ws["descriptor"])
        svc.maintain("m1")
        out = svc.step()
        st = svc.batch_status()
        return {
            "outcome": out,
            "shard_state": st["shards"]["shard-1"]["state"],
            "batch_state": st["state"],
            "rows_imported": svc.ingestion.db.execute("SELECT COUNT(*) FROM rows").fetchone()[0],
        }


if __name__ == "__main__":
    print(json.dumps(
        {
            "real_controlled_source": fixture_probe(),
            "maintenance_drain": maintenance_probe(),
            "divergent_backup": divergent_backup_probe(),
        },
        indent=2,
        ensure_ascii=False,
    ))
