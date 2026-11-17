"""Descriptor-driven batch ingestion.

The ingestion is the only component that reads files and writes SQLite.  It
identifies every unit of work by ``(batch_id, shard_id)`` (never by file name),
verifies each read against the frozen descriptor, and treats the callback as a
separate, idempotent step from the read itself:

    acquire ticket -> mark reading -> real read -> stage rows
                   -> callback (commit exactly once) -> release ticket

Because the callback is not the same event as the read, a batch can be
cancelled (or a maintenance request can land) between "bytes obtained" and
"rows committed" without importing the shard twice and without leaving the
source reference dangling.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import threading
from typing import Dict, List, Optional

from descriptor import DescriptorError, parse_csv_bytes, natural_key, shard_map

PENDING = "PENDING"
READING = "READING"
PENDING_DELIVERY = "PENDING_DELIVERY"
COMPLETE = "COMPLETE"
UNAVAILABLE = "UNAVAILABLE"
FAILED = "FAILED"
CANCELLED = "CANCELLED"

_TERMINAL = {COMPLETE, UNAVAILABLE, FAILED, CANCELLED}


class BatchError(RuntimeError):
    pass


def _connect(path: str):
    import sqlite3

    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _dedupe_tickets(tickets: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for ticket in tickets:
        key = (ticket.get("source"), ticket.get("ticket_id"))
        if key in seen:
            continue
        seen.add(key)
        out.append(ticket)
    return out


class Ingestion:
    def __init__(self, registry, db_path, sources: Dict[str, object], paths: Optional[Dict[str, str]] = None,
                 base_dir: Optional[str] = None):
        """``paths`` is source -> {shard_id: real file path} for read adapters.

        ``base_dir`` is used by ``verify`` to re-open descriptor ``origin``
        files independently of any source fixture.
        """
        self.registry = registry
        self.sources = dict(sources)
        self.paths = {name: dict(mapping) for name, mapping in (paths or {}).items()}
        self.base_dir = base_dir or os.getcwd()
        self.db = _connect(db_path)
        self._db_lock = threading.RLock()
        self._batches: Dict[str, dict] = {}
        self._batch_locks: Dict[str, threading.RLock] = {}
        self._shard_locks: Dict[tuple, threading.RLock] = {}
        self._locks_guard = threading.Lock()
        self.events: List[dict] = []
        self._event_lock = threading.Lock()
        self._init_schema()

    # -------------------------------------------------------------- persistence
    def _init_schema(self) -> None:
        with self._db_lock:
            self.db.executescript(
                """
                CREATE TABLE IF NOT EXISTS rows (
                    batch_id TEXT NOT NULL,
                    shard_id TEXT NOT NULL,
                    row_no   INTEGER NOT NULL,
                    row_id   TEXT,
                    value    INTEGER NOT NULL,
                    source   TEXT NOT NULL,
                    PRIMARY KEY (batch_id, shard_id, row_no)
                );
                CREATE TABLE IF NOT EXISTS shards_done (
                    batch_id TEXT NOT NULL,
                    shard_id TEXT NOT NULL,
                    source   TEXT NOT NULL,
                    sha256   TEXT NOT NULL,
                    rows     INTEGER NOT NULL,
                    PRIMARY KEY (batch_id, shard_id)
                );
                CREATE TABLE IF NOT EXISTS shard_log (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT, shard_id TEXT, op TEXT,
                    source TEXT, detail TEXT
                );
                CREATE TABLE IF NOT EXISTS ticket_log (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT, batch_id TEXT, shard_id TEXT,
                    op TEXT, ticket_id INTEGER, generation INTEGER, detail TEXT
                );
                """
            )
            self.db.commit()

    def _log(self, batch_id, shard_id, op, source=None, detail=None) -> None:
        with self._db_lock:
            self.db.execute(
                "INSERT INTO shard_log(batch_id,shard_id,op,source,detail) VALUES(?,?,?,?,?)",
                (batch_id, shard_id, op, source, None if detail is None else str(detail)),
            )
            self.db.commit()
        with self._event_lock:
            self.events.append(
                {"batch_id": batch_id, "shard_id": shard_id, "op": op, "source": source, "detail": detail}
            )

    def _log_ticket(self, op, ticket, detail=None) -> None:
        with self._db_lock:
            self.db.execute(
                "INSERT INTO ticket_log(source,batch_id,shard_id,op,ticket_id,generation,detail) VALUES(?,?,?,?,?,?,?)",
                (ticket.get("source"), ticket.get("batch_id"), ticket.get("shard_id"), op,
                 ticket.get("ticket_id"), ticket.get("generation"), None if detail is None else str(detail)),
            )
            self.db.commit()

    # ------------------------------------------------------------- batch state
    def register_batch(self, desc: dict) -> dict:
        with self._locks_guard:
            if desc["batch_id"] in self._batches:
                raise BatchError(f"BATCH_EXISTS:{desc['batch_id']}")
            shards = {}
            for spec in desc["shards"]:
                shards[spec["shard_id"]] = {
                    "state": PENDING, "source": None, "ticket": None, "attempts": 0,
                    "last_error": None, "rows": None, "token": 0, "record": None,
                }
            batch = {
                "batch_id": desc["batch_id"],
                "descriptor": desc,
                "shards": shards,
                "cancel_epoch": 0,
                "order": [s["shard_id"] for s in desc["shards"]],
            }
            self._batches[desc["batch_id"]] = batch
            self._batch_locks[desc["batch_id"]] = threading.RLock()
            for sid in shards:
                self._shard_locks[(desc["batch_id"], sid)] = threading.RLock()
            return batch

    def _batch(self, batch_id: str) -> dict:
        try:
            return self._batches[batch_id]
        except KeyError:
            raise BatchError(f"UNKNOWN_BATCH:{batch_id}")

    # ------------------------------------------------------------ source choice
    def _path_for(self, source: str, batch: dict, shard_id: str) -> str:
        mapped = self.paths.get(source, {}).get(shard_id)
        if mapped:
            return mapped
        spec = shard_map(batch["descriptor"])[shard_id]
        return os.path.join(self.base_dir, spec["origin"])

    def _read_raw(self, source: str, batch: dict, shard_id: str) -> bytes:
        fixture = self.sources[source]
        path = self._path_for(source, batch, shard_id)
        reader = getattr(fixture, "read_for", None)
        if reader is not None:
            return reader(source, shard_id, path)
        try:
            params = inspect.signature(fixture.read).parameters
        except (TypeError, ValueError):
            params = {}
        if len(params) >= 3:
            return fixture.read(source, shard_id, path)
        return fixture.read()

    def qualified(self, batch: dict, shard_id: str) -> List[str]:
        """Sources allowed to serve a shard, in preference order.

        A backup qualifies only when it is readable for new work *and* declares
        the descriptor snapshot.  A matching file name is never enough; the
        digest is still checked against the descriptor after the read.
        """
        desc = batch["descriptor"]
        spec = shard_map(desc)[shard_id]
        primary = desc.get("primary", "A")
        names = [primary] + [b for b in desc.get("backups", []) if b != primary]
        result = []
        for name in names:
            if name not in self.sources:
                continue
            if not self.registry.is_available(name):
                continue
            if self.registry.snapshot_of(name) != spec["snapshot"]:
                continue
            result.append(name)
        return result

    # ------------------------------------------------------------------ reading
    def _attempt(self, batch: dict, shard_id: str) -> dict:
        """One full read attempt.  Caller holds the shard lock."""
        batch_id = batch["batch_id"]
        blk = self._batch_locks[batch_id]
        with blk:
            shard = batch["shards"][shard_id]
            if shard["state"] in (COMPLETE, CANCELLED):
                return {"status": shard["state"]}
            spec = shard_map(batch["descriptor"])[shard_id]
            epoch = batch["cancel_epoch"]
            shard["attempts"] += 1
            candidates = self.qualified(batch, shard_id)
            if not candidates:
                shard["state"] = UNAVAILABLE
                shard["last_error"] = "NO_QUALIFIED_SOURCE"
                self._log(batch_id, shard_id, "unavailable", None, "NO_QUALIFIED_SOURCE")
                return {"status": "UNAVAILABLE", "reason": "NO_QUALIFIED_SOURCE"}

        reasons = []
        for source in candidates:
            with blk:
                if batch["shards"][shard_id]["state"] == CANCELLED or epoch != batch["cancel_epoch"]:
                    return {"status": "CANCELLED", "shard_id": shard_id}
            try:
                ticket = self.registry.acquire(source, batch_id, shard_id)
            except Exception as exc:  # RegistryError and friends
                reasons.append(f"{source}:{getattr(exc, 'code', exc)}")
                continue
            self._log_ticket("acquired", ticket)
            # open -> reading happens atomically before the file is opened, so
            # maintenance can only abort work that has not read anything.  A
            # ticket granted before maintenance is adopted into the draining
            # era by the registry and is allowed to finish here.
            if not self.registry.mark_reading(ticket):
                self._log_ticket("ticket-not-readable", ticket)
                self.registry.release(ticket)
                reasons.append(f"{source}:TICKET_NOT_READABLE")
                continue
            with blk:
                shard = batch["shards"][shard_id]
                if shard["state"] == CANCELLED or epoch != batch["cancel_epoch"]:
                    # Cancelled between acquiring the ticket and opening the
                    # file: never start a real read, settle the reference now.
                    self.registry.release(ticket)
                    shard["state"] = CANCELLED
                    shard["last_error"] = "BATCH_CANCELLED"
                    self._log(batch_id, shard_id, "read-aborted-cancelled", source, ticket["ticket_id"])
                    return {"status": "CANCELLED"}
                shard["token"] += 1
                token = shard["token"]
                shard["state"] = READING
                shard["source"] = source
                shard["ticket"] = ticket
            self._log(batch_id, shard_id, "read-start", source, ticket["ticket_id"])
            try:
                raw = self._read_raw(source, batch, shard_id)
            except Exception as exc:
                self._log_ticket("read-failed", ticket, exc)
                self.registry.release(ticket)
                reasons.append(f"{source}:READ_FAILED:{exc}")
                with blk:
                    batch["shards"][shard_id]["last_error"] = reasons[-1]
                self._log(batch_id, shard_id, "read-failed", source, exc)
                continue
            digest = hashlib.sha256(raw).hexdigest()
            with blk:
                cancelled_now = batch["shards"][shard_id]["state"] == CANCELLED or epoch != batch["cancel_epoch"]
            if cancelled_now:
                # Read completed but the batch was cancelled meanwhile: do not
                # import, settle the source reference exactly once.
                self.registry.release(ticket)
                with blk:
                    shard = batch["shards"][shard_id]
                    shard["state"] = CANCELLED
                    shard["last_error"] = "BATCH_CANCELLED"
                    shard["ticket"] = None
                self._log(batch_id, shard_id, "read-cancelled", source, ticket["ticket_id"])
                return {"status": "CANCELLED"}
            if len(raw) != spec["bytes"] or digest != spec["sha256"]:
                # Same name is not identity: refuse and fall through.
                self._log_ticket("digest-mismatch", ticket, digest)
                self.registry.release(ticket)
                reasons.append(f"{source}:DIGEST_MISMATCH")
                with blk:
                    batch["shards"][shard_id]["last_error"] = reasons[-1]
                self._log(batch_id, shard_id, "verify-mismatch", source, digest)
                continue
            try:
                _, rows = parse_csv_bytes(raw)
            except DescriptorError as exc:
                self._log_ticket("parse-failed", ticket, exc)
                self.registry.release(ticket)
                reasons.append(f"{source}:PARSE_FAILED:{exc}")
                continue
            if len(rows) != spec["row_count"]:
                self._log_ticket("row-count-mismatch", ticket, len(rows))
                self.registry.release(ticket)
                reasons.append(f"{source}:ROW_COUNT_MISMATCH")
                with blk:
                    batch["shards"][shard_id]["state"] = UNAVAILABLE
                    batch["shards"][shard_id]["last_error"] = reasons[-1]
                continue
            record = {
                "batch_id": batch_id, "shard_id": shard_id, "source": source,
                "ticket": ticket, "token": token, "epoch": epoch, "digest": digest,
                "rows": rows, "delivered": False,
            }
            with blk:
                shard = batch["shards"][shard_id]
                if shard["state"] == CANCELLED or epoch != batch["cancel_epoch"]:
                    self.registry.release(ticket)
                    shard["state"] = CANCELLED
                    shard["last_error"] = "BATCH_CANCELLED"
                    self._log(batch_id, shard_id, "read-cancelled", source, ticket["ticket_id"])
                    return {"status": "CANCELLED"}
                shard["state"] = PENDING_DELIVERY
                shard["source"] = source
                shard["ticket"] = ticket
                shard["record"] = record
            self._log(batch_id, shard_id, "read-end", source, len(raw))
            return {"status": "READ", "record": record}

        with blk:
            shard = batch["shards"][shard_id]
            if shard["state"] not in (COMPLETE, CANCELLED):
                shard["state"] = UNAVAILABLE
                shard["last_error"] = ";".join(reasons) or "NO_SOURCE"
            self._log(batch_id, shard_id, "unavailable", None, ";".join(reasons))
        return {"status": "UNAVAILABLE", "reasons": reasons}

    # ---------------------------------------------------------------- callback
    def _deliver(self, record: dict) -> dict:
        """Idempotent completion callback.  Commits at most once per shard.

        The ticket is released exactly once no matter how many times this runs
        (duplicate callback, cancel interleaving, stale token).  The release
        may itself trigger a real close if the source is draining, which is the
        intended coupling between "shard done" and "source closable".
        """
        batch = self._batch(record["batch_id"])
        batch_id, shard_id = record["batch_id"], record["shard_id"]
        source, ticket = record["source"], record["ticket"]
        blk = self._batch_locks[batch_id]
        release_now = False
        with blk:
            shard = batch["shards"][shard_id]
            if shard["state"] == CANCELLED or batch["cancel_epoch"] != record["epoch"]:
                result = {"status": "CANCELLED", "shard_id": shard_id}
                record["delivered"] = True
                shard["state"] = CANCELLED
                shard["last_error"] = "BATCH_CANCELLED"
                shard["record"] = None
                shard["ticket"] = None
                release_now = not record.get("released")
                self._log(batch_id, shard_id, "callback-cancelled", source, ticket["ticket_id"])
            elif record["delivered"] or shard["state"] == COMPLETE:
                result = {"status": "DUPLICATE", "shard_id": shard_id}
                record["delivered"] = True
                release_now = not record.get("released")
            elif record["token"] != shard["token"]:
                result = {"status": "STALE_CALLBACK", "shard_id": shard_id}
                record["delivered"] = True
                release_now = not record.get("released")
            else:
                self._commit(record)
                record["delivered"] = True
                shard["state"] = COMPLETE
                shard["source"] = source
                shard["rows"] = len(record["rows"])
                shard["ticket"] = None
                shard["record"] = None
                shard["last_error"] = None
                result = {"status": "COMPLETE", "shard_id": shard_id,
                          "rows": len(record["rows"]), "source": source}
                release_now = not record.get("released")
            if release_now:
                record["released"] = True
        if release_now:
            # Release outside the batch lock: a release can run a real fixture
            # close, and we do not want to hold ingestion state during I/O.
            self.registry.release(ticket)
            self._log_ticket("released", ticket, result["status"])
        return result

    def _commit(self, record: dict) -> None:
        batch_id, shard_id, source = record["batch_id"], record["shard_id"], record["source"]
        with self._db_lock:
            if self.db.execute("SELECT 1 FROM shards_done WHERE batch_id=? AND shard_id=?",
                               (batch_id, shard_id)).fetchone():
                return
            try:
                self.db.execute("BEGIN IMMEDIATE")
                self.db.executemany(
                    "INSERT INTO rows(batch_id,shard_id,row_no,row_id,value,source) VALUES(?,?,?,?,?,?)",
                    [(batch_id, shard_id, r["row_no"], r["row_id"], r["value"], source) for r in record["rows"]],
                )
                self.db.execute(
                    "INSERT INTO shards_done(batch_id,shard_id,source,sha256,rows) VALUES(?,?,?,?,?)",
                    (batch_id, shard_id, source, record["digest"], len(record["rows"])),
                )
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
        self._log(batch_id, shard_id, "commit", source, len(record["rows"]))

    def deliver(self, batch_id: str, shard_id: str) -> dict:
        """External completion callback for a staged (read-not-committed) shard."""
        batch = self._batch(batch_id)
        with self._batch_locks[batch_id]:
            shard = batch["shards"][shard_id]
            if shard["state"] == COMPLETE:
                return {"status": "DUPLICATE", "shard_id": shard_id}
            record = shard.get("record")
            if record is None:
                return {"status": "NO_PENDING", "shard_id": shard_id}
        return self._deliver(record)

    # ------------------------------------------------------------- public drive
    def _run(self, batch_id: str, shard_id: str, deliver: bool, block: bool) -> dict:
        slk = self._shard_locks[(batch_id, shard_id)]
        if not slk.acquire(blocking=block):
            return {"status": "IN_FLIGHT", "shard_id": shard_id}
        try:
            outcome = self._attempt(self._batch(batch_id), shard_id)
            if outcome["status"] == "READ" and deliver:
                return self._deliver(outcome["record"])
            return outcome
        finally:
            slk.release()

    def dispatch(self, batch_id: str, shard_id: str, deliver: bool = True) -> threading.Thread:
        """Start one attempt on a shard in a worker thread.

        A second dispatcher racing on the same shard gets ``IN_FLIGHT`` from a
        non-blocking lock instead of starting a duplicate read.
        """

        def run():
            try:
                self._run(batch_id, shard_id, deliver, block=False)
            except Exception as exc:  # never let a worker die silently
                self._log(batch_id, shard_id, "worker-error", None, exc)

        thread = threading.Thread(target=run, name=f"dispatch-{batch_id}-{shard_id}", daemon=True)
        thread.start()
        return thread

    def step(self, batch_id: str) -> dict:
        """Synchronously advance the next unfinished shard by one full attempt.

        ``COMPLETE`` and ``CANCELLED`` shards are skipped.  An ``UNAVAILABLE``
        shard is retried (it is "kept waiting", never silently completed).
        """
        batch = self._batch(batch_id)
        for shard_id in batch["order"]:
            with self._batch_locks[batch_id]:
                shard = batch["shards"][shard_id]
                if shard["state"] in (COMPLETE, CANCELLED):
                    continue
                if shard["state"] == PENDING_DELIVERY and shard.get("record") is not None:
                    record = shard["record"]
                    pending = True
                else:
                    pending = False
            if pending:
                return self._deliver(record)
            return self._run(batch_id, shard_id, deliver=True, block=True)
        return {"status": "BATCH_FINISHED"}

    def step_all(self, batch_id: str) -> List[dict]:
        """Advance every unfinished shard once (useful for deterministic runs)."""
        results = []
        batch = self._batch(batch_id)
        for shard_id in batch["order"]:
            with self._batch_locks[batch_id]:
                state = batch["shards"][shard_id]["state"]
            if state in (COMPLETE, CANCELLED):
                continue
            results.append(self.step(batch_id))
        return results

    # ---------------------------------------------------------------- cancelling
    def cancel_batch(self, batch_id: str, reason: str = "cancelled") -> dict:
        """Cancel a batch and settle *all* source references it still holds.

        Tickets that have already acquired a source are aborted for shards that
        have not started reading; a shard whose real read is already running
        finishes its read but its callback turns into a cancellation, so its
        ticket is released exactly once.
        """
        batch = self._batch(batch_id)
        release_list: List[dict] = []
        with self._batch_locks[batch_id]:
            batch["cancel_epoch"] += 1
            cancelled = []
            for shard_id in batch["order"]:
                shard = batch["shards"][shard_id]
                if shard["state"] == COMPLETE:
                    continue
                cancelled.append(shard_id)
                record = shard.get("record")
                if record is not None and not record["delivered"]:
                    # Bytes were obtained but the callback had not committed.
                    # Cancel before commit: drop it and release the ticket once.
                    record["delivered"] = True
                    record["released"] = True
                    release_list.append(record["ticket"])
                    shard["record"] = None
                    shard["ticket"] = None
                if shard["state"] == READING:
                    # A real read is physically running.  Do not release the
                    # source reference early; its worker observes the bumped
                    # epoch and settles the ticket after the read returns.
                    shard["last_error"] = "BATCH_CANCELLED"
                    continue
                shard["state"] = CANCELLED
                shard["last_error"] = "BATCH_CANCELLED"
                if shard.get("ticket") is not None:
                    release_list.append(shard["ticket"])
                    shard["ticket"] = None
            aborted = []
            for source in list(self.sources):
                aborted.extend(self.registry.abort_open_tickets(source, reason="batch-cancel", batch_id=batch_id))
            for ticket in aborted:
                self._log_ticket("aborted", ticket, reason)
        for ticket in _dedupe_tickets(release_list):
            self.registry.release(ticket)
            self._log_ticket("released", ticket, "cancel-batch")
        for source in list(self.sources):
            self.registry.tick(source)
        self._log(batch_id, None, "cancel-batch", None, reason)
        return {
            "batch_id": batch_id,
            "cancelled": cancelled,
            "settled_tickets": len(release_list) + len(aborted),
        }

    # ------------------------------------------------------------------ status
    def _rows_for(self, batch_id: str) -> int:
        with self._db_lock:
            return self.db.execute("SELECT COUNT(*) FROM rows WHERE batch_id=?", (batch_id,)).fetchone()[0]

    def batch_status(self, batch_id: str) -> dict:
        batch = self._batch(batch_id)
        with self._batch_locks[batch_id]:
            shards = {
                sid: {
                    "state": s["state"], "source": s["source"], "attempts": s["attempts"],
                    "rows": s["rows"], "last_error": s["last_error"],
                }
                for sid, s in batch["shards"].items()
            }
            incomplete = [sid for sid in batch["order"] if shards[sid]["state"] != COMPLETE]
            unavailable = [sid for sid in batch["order"] if shards[sid]["state"] == UNAVAILABLE]
            cancelled = [sid for sid in batch["order"] if shards[sid]["state"] == CANCELLED]
            complete = [sid for sid in batch["order"] if shards[sid]["state"] == COMPLETE]
            result = {
                "batch_id": batch_id,
                "snapshot": batch["descriptor"]["snapshot"],
                "shards": shards,
                "complete": complete,
                "incomplete": incomplete,
                "unavailable": unavailable,
                "cancelled": cancelled,
                "cancel_epoch": batch["cancel_epoch"],
                "rows": self._rows_for(batch_id),
            }
            if not incomplete:
                result["state"] = "COMPLETE"
            elif not [s for s in batch["order"] if shards[s]["state"] in (PENDING, READING, PENDING_DELIVERY)]:
                result["state"] = "BLOCKED"
            else:
                result["state"] = "IN_PROGRESS"
            return result

    # ------------------------------------------------------------------ verify
    def verify(self, batch_id: str) -> dict:
        """Re-open raw origin files and check every row against SQLite.

        Deliberately independent of the fixture/source path: it reads the
        descriptor ``origin`` files.  Distinguishes "complete but zero rows"
        from "never obtained"; only ``incomplete`` shards are reported as
        missing, and a descriptor-legal zero-row shard is COMPLETE and checks
        out with a total of 0.
        """
        batch = self._batch(batch_id)
        desc = batch["descriptor"]
        per_shard = {}
        mismatch = []
        origin_total = 0
        db_total = 0
        for spec in desc["shards"]:
            sid = spec["shard_id"]
            path = os.path.join(self.base_dir, spec["origin"])
            row = {
                "shard_id": sid, "origin": spec["origin"],
                "expected_rows": spec["row_count"], "expected_bytes": spec["bytes"],
                "expected_sha256": spec["sha256"],
            }
            with self._batch_locks[batch_id]:
                row["state"] = batch["shards"][sid]["state"]
            try:
                with open(path, "rb") as fh:
                    raw = fh.read()
            except OSError as exc:
                row["error"] = f"ORIGIN_MISSING:{exc}"
                row["descriptor_intact"] = False
                per_shard[sid] = row
                mismatch.append(sid)
                continue
            digest = hashlib.sha256(raw).hexdigest()
            row["bytes"] = len(raw)
            row["sha256"] = digest
            row["descriptor_intact"] = len(raw) == spec["bytes"] and digest == spec["sha256"]
            if not row["descriptor_intact"]:
                row["error"] = "DESCRIPTOR_MISMATCH"
                mismatch.append(sid)
            try:
                _, rows = parse_csv_bytes(raw)
            except DescriptorError as exc:
                row["error"] = f"PARSE_FAILED:{exc}"
                per_shard[sid] = row
                mismatch.append(sid)
                continue
            row["rows"] = len(rows)
            row["total"] = sum(r["value"] for r in rows)
            origin_total += row["total"]
            with self._db_lock:
                db_rows = self.db.execute(
                    "SELECT row_no,row_id,value,source FROM rows WHERE batch_id=? AND shard_id=? ORDER BY row_no",
                    (batch_id, sid),
                ).fetchall()
                done = self.db.execute(
                    "SELECT source,sha256,rows FROM shards_done WHERE batch_id=? AND shard_id=?",
                    (batch_id, sid),
                ).fetchone()
            db_total += sum(r[2] for r in db_rows)
            row["db_rows"] = len(db_rows)
            row["complete"] = row["state"] == COMPLETE
            if done is not None:
                row["db_source"], row["db_sha256"], row["db_committed_rows"] = done
            if not row["complete"]:
                row["row_check"] = "NOT_COMPLETE"
            elif len(db_rows) != len(rows):
                row["row_check"] = "COUNT_MISMATCH"
                mismatch.append(sid)
            else:
                bad = 0
                for expected, got in zip(rows, db_rows):
                    if got[0] != expected["row_no"] or got[1] != expected["row_id"] or int(got[2]) != expected["value"]:
                        bad += 1
                row["row_check"] = "OK" if bad == 0 else f"ROW_MISMATCH:{bad}"
                if bad:
                    mismatch.append(sid)
                real_total = sum(r[2] for r in db_rows)
                row["db_total"] = real_total
                if row["total"] != real_total:
                    row["total_check"] = "MISMATCH"
                    mismatch.append(sid)
                else:
                    row["total_check"] = "OK"
            per_shard[sid] = row
        with self._batch_locks[batch_id]:
            incomplete = [sid for sid in batch["order"] if batch["shards"][sid]["state"] != COMPLETE]
        descriptor_sha = hashlib.sha256(
            json.dumps(desc, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return {
            "batch_id": batch_id,
            "snapshot": desc["snapshot"],
            "descriptor_sha256": descriptor_sha,
            "shards": per_shard,
            "origin_total": origin_total,
            "db_total": db_total,
            "totals_match": origin_total == db_total,
            "complete": [sid for sid in batch["order"] if sid not in incomplete],
            "incomplete": incomplete,
            "mismatched": sorted(set(mismatch), key=natural_key),
            "ok": not incomplete and not mismatch and origin_total == db_total,
        }
