"""Immutable billing seals and idempotent reconciliation.

The settlement service never rewrites a bill.  ``seal(month, k)`` freezes the
per-meter receivable that was communicated to the customer, together with the
knowledge cutoff it was derived from.  ``reconcile(month, k)`` compares that
frozen baseline plus every adjustment that has *actually been committed*
against the latest permitted facts, and publishes only the remaining net
difference per meter.

Knowledge only moves forward within a month.  An equal cutoff replays the
previous decision; an older cutoff returns an explicit STALE receipt and never
opens a new charge.  Publishing is serialised in a single ``BEGIN IMMEDIATE``
transaction, so two reconcilers that both observed the same old posted amount
cannot both book the same supplement: whichever commits first advances the
month version, and the second sees the new cutoff and replays it.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

from facts import Facts, normalize_month

__all__ = [
    "Settlement",
    "SettlementError",
    "NotSealed",
]


class SettlementError(ValueError):
    """Base class for settlement rejections."""


class NotSealed(SettlementError):
    """A month must be sealed before it can be reconciled."""


def _canonical_lines(lines: dict[str, int]) -> str:
    return json.dumps(
        {meter: int(amount) for meter, amount in sorted(lines.items())},
        sort_keys=True,
        separators=(",", ":"),
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Settlement:
    """Persistent seal + reconciliation ledger."""

    def __init__(self, path, facts: Facts):
        self.path = str(path)
        self.facts = facts
        self._lock = threading.RLock()
        self.db = sqlite3.connect(
            self.path, timeout=30.0, isolation_level=None, check_same_thread=False
        )
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout=30000")
        try:
            self.db.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            pass
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS bills (
                bill_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                month        TEXT NOT NULL UNIQUE,
                sealed_k     INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                content_json TEXT NOT NULL,
                created_at   TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bill_lines (
                bill_id INTEGER NOT NULL,
                meter   TEXT NOT NULL,
                amount  INTEGER NOT NULL,
                sources_json TEXT NOT NULL,
                PRIMARY KEY (bill_id, meter)
            );
            CREATE TABLE IF NOT EXISTS decisions (
                decision_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                month           TEXT NOT NULL,
                k               INTEGER NOT NULL,
                prev_k          INTEGER NOT NULL,
                status          TEXT NOT NULL,
                adjustment_id   INTEGER,
                target_total    INTEGER NOT NULL,
                delta_total     INTEGER NOT NULL,
                cumulative_total INTEGER NOT NULL,
                receipt_json    TEXT NOT NULL,
                created_at      TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS decisions_month_k
                ON decisions(month, k);
            CREATE TABLE IF NOT EXISTS adjustments (
                adjustment_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_id     INTEGER NOT NULL,
                month           TEXT NOT NULL,
                k               INTEGER NOT NULL,
                delta_total     INTEGER NOT NULL,
                cumulative_total INTEGER NOT NULL,
                created_at      TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS adjustments_month
                ON adjustments(month);
            CREATE TABLE IF NOT EXISTS adjustment_lines (
                adjustment_id  INTEGER NOT NULL,
                meter          TEXT NOT NULL,
                target         INTEGER NOT NULL,
                initial_bill   INTEGER NOT NULL,
                prior_adjustments INTEGER NOT NULL,
                delta          INTEGER NOT NULL,
                sources_json   TEXT NOT NULL,
                PRIMARY KEY (adjustment_id, meter)
            );
            CREATE TABLE IF NOT EXISTS month_state (
                month            TEXT PRIMARY KEY,
                bill_id          INTEGER NOT NULL,
                sealed_k         INTEGER NOT NULL,
                last_k           INTEGER NOT NULL,
                version          INTEGER NOT NULL,
                last_decision_id INTEGER
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        # Sealed bills and their posted lines are write-once at the storage
        # layer as well as the API layer.
        self.db.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS bills_no_update
                BEFORE UPDATE ON bills BEGIN SELECT RAISE(ABORT, 'bills are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS bills_no_delete
                BEFORE DELETE ON bills BEGIN SELECT RAISE(ABORT, 'bills are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS bill_lines_no_update
                BEFORE UPDATE ON bill_lines BEGIN SELECT RAISE(ABORT, 'bill lines are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS bill_lines_no_delete
                BEFORE DELETE ON bill_lines BEGIN SELECT RAISE(ABORT, 'bill lines are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS adjustments_no_update
                BEFORE UPDATE ON adjustments BEGIN SELECT RAISE(ABORT, 'adjustments are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS adjustments_no_delete
                BEFORE DELETE ON adjustments BEGIN SELECT RAISE(ABORT, 'adjustments are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS adjustment_lines_no_update
                BEFORE UPDATE ON adjustment_lines BEGIN SELECT RAISE(ABORT, 'adjustment lines are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS adjustment_lines_no_delete
                BEFORE DELETE ON adjustment_lines BEGIN SELECT RAISE(ABORT, 'adjustment lines are immutable'); END;
            """
        )
        self.db.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('schema', '1')")
        self.db.commit()

    # ------------------------------------------------------------------ seal

    def seal(self, month: str, k: int) -> dict:
        """Freeze the initial bill for ``month`` using facts at ``k``.

        A month seals exactly once.  Every later call replays that same
        immutable decision, even if a different (for example later) cutoff is
        requested: a sealed bill may never be overwritten by newer knowledge.
        """

        month = normalize_month(month)
        cutoff = self.facts.resolve_k(k)

        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                existing = self.db.execute(
                    "SELECT * FROM bills WHERE month = ?", (month,)
                ).fetchone()
                if existing is not None:
                    self.db.execute("COMMIT")
                    receipt = self._bill_receipt(existing, replayed=True)
                    receipt["requested_k"] = cutoff
                    if cutoff != int(existing["sealed_k"]):
                        receipt["seal_replay_reason"] = (
                            f"month {month} is sealed at k={existing['sealed_k']};"
                            f" requested k={cutoff} was ignored so the issued bill"
                            " is not overwritten"
                        )
                    return receipt

                breakdown = self.facts.breakdown(cutoff, month)
                totals = {meter: info["total"] for meter, info in breakdown.items()}
                sources = {meter: info["sources"] for meter, info in breakdown.items()}
                content_json = _canonical_lines(totals)
                content_hash = hashlib.sha256(content_json.encode("utf-8")).hexdigest()

                cur = self.db.execute(
                    "INSERT INTO bills"
                    "(month, sealed_k, content_hash, content_json, created_at)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (month, cutoff, content_hash, content_json, _now()),
                )
                bill_id = int(cur.lastrowid)
                for meter in sorted(totals):
                    self.db.execute(
                        "INSERT INTO bill_lines"
                        "(bill_id, meter, amount, sources_json) VALUES (?, ?, ?, ?)",
                        (
                            bill_id,
                            meter,
                            int(totals[meter]),
                            json.dumps(sources[meter], sort_keys=True, ensure_ascii=False),
                        ),
                    )
                self.db.execute(
                    "INSERT INTO month_state"
                    "(month, bill_id, sealed_k, last_k, version, last_decision_id)"
                    " VALUES (?, ?, ?, ?, 0, NULL)",
                    (month, bill_id, cutoff, cutoff),
                )
                self.db.execute("COMMIT")
            except BaseException:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

        row = self.db.execute("SELECT * FROM bills WHERE month = ?", (month,)).fetchone()
        return self._bill_receipt(row, replayed=False)

    # ------------------------------------------------------------- reconcile

    def reconcile(self, month: str, k: int) -> dict:
        """Publish the net difference between facts at ``k`` and what is booked.

        The whole read-compute-publish cycle runs under ``BEGIN IMMEDIATE`` and
        re-reads the month version inside the transaction.  That is what makes
        two callers who both saw the old posted amount safe: the second sees
        the first's committed cutoff/version and replays instead of double
        booking.
        """

        month = normalize_month(month)
        cutoff = self.facts.resolve_k(k)

        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                state = self.db.execute(
                    "SELECT * FROM month_state WHERE month = ?", (month,)
                ).fetchone()
                if state is None:
                    raise NotSealed(f"month {month} has not been sealed")

                last_k = int(state["last_k"])
                version = int(state["version"])

                if cutoff < last_k:
                    self.db.execute("COMMIT")
                    return self._stale_receipt(month, cutoff, last_k)

                if cutoff == last_k and state["last_decision_id"] is not None:
                    decision = self._decision_row(int(state["last_decision_id"]))
                    self.db.execute("COMMIT")
                    return self._replay_receipt(decision, reason="same knowledge cutoff")

                # --- derive the target at the bound cutoff -------------------
                breakdown = self.facts.breakdown(cutoff, month)
                target = {meter: info["total"] for meter, info in breakdown.items()}
                sources = {meter: info["sources"] for meter, info in breakdown.items()}

                bill = self.db.execute(
                    "SELECT * FROM bills WHERE month = ?", (month,)
                ).fetchone()
                initial = {
                    row["meter"]: int(row["amount"])
                    for row in self.db.execute(
                        "SELECT meter, amount FROM bill_lines WHERE bill_id = ?",
                        (bill["bill_id"],),
                    ).fetchall()
                }
                prior = self._prior_adjustments_unlocked(month)

                meters = sorted(set(target) | set(initial) | set(prior))
                lines = []
                for meter in meters:
                    billed = int(initial.get(meter, 0))
                    adjusted = int(prior.get(meter, 0))
                    cumulative = billed + adjusted
                    tgt = int(target.get(meter, 0))
                    lines.append(
                        {
                            "meter": meter,
                            "target": tgt,
                            "initial_bill": billed,
                            "prior_adjustments": adjusted,
                            "cumulative_settled": cumulative,
                            "delta": tgt - cumulative,
                            "sources": sources.get(meter, []),
                        }
                    )

                delta_total = sum(line["delta"] for line in lines)
                target_total = sum(line["target"] for line in lines)
                cumulative_before = sum(line["cumulative_settled"] for line in lines)
                cumulative_after = cumulative_before + delta_total
                changed = any(line["delta"] != 0 for line in lines)

                receipt = {
                    "month": month,
                    "k": cutoff,
                    "prev_k": last_k,
                    "bill_id": int(bill["bill_id"]),
                    "sealed_k": int(bill["sealed_k"]),
                    "status": "published" if changed else "no_change",
                    "delta": delta_total,
                    "posted_this_round": delta_total,
                    "replayed": False,
                    "target_total": target_total,
                    "cumulative": cumulative_after,
                    "version": version + 1,
                    "adjustment_id": None,
                    "decision_id": None,
                    "lines": lines,
                }

                cur = self.db.execute(
                    "INSERT INTO decisions"
                    "(month, k, prev_k, status, adjustment_id, target_total, delta_total,"
                    " cumulative_total, receipt_json, created_at)"
                    " VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)",
                    (
                        month,
                        cutoff,
                        last_k,
                        receipt["status"],
                        target_total,
                        delta_total,
                        cumulative_after,
                        "{}",  # filled below once ids are known
                        _now(),
                    ),
                )
                decision_id = int(cur.lastrowid)
                receipt["decision_id"] = decision_id

                adjustment_id = None
                if changed:
                    cur = self.db.execute(
                        "INSERT INTO adjustments"
                        "(decision_id, month, k, delta_total, cumulative_total, created_at)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        (decision_id, month, cutoff, delta_total, cumulative_after, _now()),
                    )
                    adjustment_id = int(cur.lastrowid)
                    for line in lines:
                        self.db.execute(
                            "INSERT INTO adjustment_lines"
                            "(adjustment_id, meter, target, initial_bill,"
                            " prior_adjustments, delta, sources_json)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (
                                adjustment_id,
                                line["meter"],
                                line["target"],
                                line["initial_bill"],
                                line["prior_adjustments"],
                                line["delta"],
                                json.dumps(
                                    line["sources"], sort_keys=True, ensure_ascii=False
                                ),
                            ),
                        )
                    receipt["adjustment_id"] = adjustment_id

                self.db.execute(
                    "UPDATE decisions SET adjustment_id = ?, receipt_json = ?"
                    " WHERE decision_id = ?",
                    (
                        adjustment_id,
                        json.dumps(receipt, sort_keys=True, ensure_ascii=False),
                        decision_id,
                    ),
                )
                self.db.execute(
                    "UPDATE month_state SET last_k = ?, version = ?, last_decision_id = ?"
                    " WHERE month = ?",
                    (cutoff, version + 1, decision_id, month),
                )
                self.db.execute("COMMIT")
            except BaseException:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

        return receipt

    # -------------------------------------------------------------- queries

    def statement(self, month: str) -> dict:
        """Full audit view: seal, every decision, posted ledger and balances."""

        month = normalize_month(month)
        bill = self.db.execute("SELECT * FROM bills WHERE month = ?", (month,)).fetchone()
        if bill is None:
            raise NotSealed(f"month {month} has not been sealed")
        state = self.db.execute(
            "SELECT * FROM month_state WHERE month = ?", (month,)
        ).fetchone()
        initial = {
            row["meter"]: int(row["amount"])
            for row in self.db.execute(
                "SELECT meter, amount FROM bill_lines WHERE bill_id = ?",
                (bill["bill_id"],),
            ).fetchall()
        }
        decisions = [
            json.loads(row["receipt_json"])
            for row in self.db.execute(
                "SELECT * FROM decisions WHERE month = ? ORDER BY decision_id ASC",
                (month,),
            ).fetchall()
        ]
        adjustments = []
        for row in self.db.execute(
            "SELECT * FROM adjustments WHERE month = ? ORDER BY adjustment_id ASC",
            (month,),
        ).fetchall():
            adjustments.append(
                {
                    "adjustment_id": int(row["adjustment_id"]),
                    "decision_id": int(row["decision_id"]),
                    "k": int(row["k"]),
                    "delta": int(row["delta_total"]),
                    "cumulative": int(row["cumulative_total"]),
                    "lines": [
                        {
                            "meter": line["meter"],
                            "target": int(line["target"]),
                            "initial_bill": int(line["initial_bill"]),
                            "prior_adjustments": int(line["prior_adjustments"]),
                            "delta": int(line["delta"]),
                            "sources": json.loads(line["sources_json"]),
                        }
                        for line in self.db.execute(
                            "SELECT * FROM adjustment_lines"
                            " WHERE adjustment_id = ? ORDER BY meter ASC",
                            (row["adjustment_id"],),
                        ).fetchall()
                    ],
                }
            )
        prior = self._prior_adjustments(month)
        meters = sorted(set(initial) | set(prior))
        balances = {
            meter: {
                "initial_bill": int(initial.get(meter, 0)),
                "adjustments": int(prior.get(meter, 0)),
                "cumulative_settled": int(initial.get(meter, 0)) + int(prior.get(meter, 0)),
            }
            for meter in meters
        }
        target_at_last = self.facts.as_of(int(state["last_k"]), month)
        return {
            "month": month,
            "bill": self._bill_receipt(bill, replayed=False),
            "sealed_k": int(bill["sealed_k"]),
            "last_k": int(state["last_k"]),
            "version": int(state["version"]),
            "balances": balances,
            "total_initial": sum(initial.values()),
            "total_adjustments": sum(prior.values()),
            "total_cumulative": sum(initial.values()) + sum(prior.values()),
            "target_at_last_k": target_at_last,
            "adjustments": adjustments,
            "decisions": decisions,
        }

    def posted(self, month: str, meter: Optional[str] = None) -> int:
        """Committed adjustment total for a month (optionally one meter)."""

        month = normalize_month(month)
        prior = self._prior_adjustments(month)
        if meter is not None:
            return int(prior.get(meter, 0))
        return int(sum(prior.values()))

    def close(self) -> None:
        with self._lock:
            self.db.close()

    # ------------------------------------------------------------ internals

    def _prior_adjustments(self, month: str) -> dict[str, int]:
        with self._lock:
            return self._prior_adjustments_unlocked(month)

    def _prior_adjustments_unlocked(self, month: str) -> dict[str, int]:
        rows = self.db.execute(
            "SELECT l.meter AS meter, SUM(l.delta) AS total"
            " FROM adjustment_lines l JOIN adjustments a"
            " ON a.adjustment_id = l.adjustment_id"
            " WHERE a.month = ? GROUP BY l.meter",
            (month,),
        ).fetchall()
        return {row["meter"]: int(row["total"]) for row in rows}

    def _decision_row(self, decision_id: int) -> sqlite3.Row:
        return self.db.execute(
            "SELECT * FROM decisions WHERE decision_id = ?", (decision_id,)
        ).fetchone()

    def _replay_receipt(self, decision: sqlite3.Row, reason: str) -> dict:
        receipt = json.loads(decision["receipt_json"])
        replay = dict(receipt)
        # Nothing new is booked on replay, so the round delta is zero while the
        # originally published delta is preserved for the audit trail.
        replay["replayed"] = True
        replay["status"] = "replayed"
        replay["original_status"] = receipt.get("status")
        replay["original_delta"] = receipt.get("delta")
        replay["delta"] = 0
        replay["posted_this_round"] = 0
        replay["lines"] = [
            {
                **line,
                "original_delta": line.get("delta"),
                "delta": 0,
            }
            for line in receipt.get("lines", [])
        ]
        replay["replay_reason"] = reason
        return replay

    def _stale_receipt(self, month: str, k: int, last_k: int) -> dict:
        # If a decision was already published at exactly this older cutoff,
        # surface that original decision alongside an explicit STALE marker.
        original = None
        row = self.db.execute(
            "SELECT * FROM decisions WHERE month = ? AND k = ?"
            " ORDER BY decision_id DESC LIMIT 1",
            (month, k),
        ).fetchone()
        if row is not None:
            original = json.loads(row["receipt_json"])
        prior = self._prior_adjustments_unlocked(month)
        return {
            "month": month,
            "k": k,
            "requested_k": k,
            "last_k": last_k,
            "status": "stale",
            "stale": True,
            "delta": 0,
            "posted_this_round": 0,
            "replayed": False,
            "cumulative": int(sum(prior.values()))
            + self._initial_total_unlocked(month),
            "adjustment_id": None,
            "decision_id": None if row is None else int(row["decision_id"]),
            "original_decision": original,
            "reason": f"knowledge cutoff {k} is older than committed {last_k}",
        }

    def _initial_total_unlocked(self, month: str) -> int:
        row = self.db.execute(
            "SELECT COALESCE(SUM(l.amount), 0) AS total FROM bill_lines l"
            " JOIN bills b ON b.bill_id = l.bill_id WHERE b.month = ?",
            (month,),
        ).fetchone()
        return int(row["total"])

    def _bill_receipt(self, bill: sqlite3.Row, *, replayed: bool) -> dict:
        lines = self.db.execute(
            "SELECT meter, amount, sources_json FROM bill_lines"
            " WHERE bill_id = ? ORDER BY meter ASC",
            (bill["bill_id"],),
        ).fetchall()
        return {
            "bill_id": int(bill["bill_id"]),
            "month": bill["month"],
            "k": int(bill["sealed_k"]),
            "sealed_k": int(bill["sealed_k"]),
            "status": "sealed",
            "replayed": replayed,
            "content_hash": bill["content_hash"],
            "lines": [
                {
                    "meter": row["meter"],
                    "amount": int(row["amount"]),
                    "sources": json.loads(row["sources_json"]),
                }
                for row in lines
            ],
            "total": sum(int(row["amount"]) for row in lines),
            "created_at": bill["created_at"],
        }
