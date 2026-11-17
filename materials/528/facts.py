"""Append-only, bitemporal metering facts.

Every accepted event gets a globally increasing ``knowledge_seq`` (a committed
contiguous prefix).  Daily validity is stored as half-open intervals
``[a, b)``; a ``set`` supplies one integer-cent amount per day and a
``retract`` supplies zero for every day in its interval.

The log is append-only.  ``as_of(k, month)`` replays the log up to sequence
``k``, so old knowledge can be reconstructed exactly rather than being
overwritten by the current value.  Resolution is per (meter, day): the event
with the greatest ``seq <= k`` that covers the day wins.

Idempotency is by ``event_id``: re-sending the identical canonical content
returns the original sequence; reusing the id with different content is
rejected before anything is written (and therefore consumes no sequence).
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
from datetime import date, timedelta
from typing import Any, Optional

__all__ = [
    "Facts",
    "FactsError",
    "EventConflict",
    "KnowledgeCutoffError",
    "ValidationError",
    "normalize_month",
    "month_bounds",
]

_MONTH_RE = re.compile(r"^(\d{4})-(\d{1,2})$")
_ONE_DAY = timedelta(days=1)
_MAX_RANGE_DAYS = 62
_MAX_MONTHS_SPANNED = 2


class FactsError(ValueError):
    """Base class for rejected facts operations."""


class ValidationError(FactsError):
    """The supplied event is not a legal fact."""


class EventConflict(FactsError):
    """An event id was reused with different content."""


class KnowledgeCutoffError(FactsError):
    """The requested knowledge cutoff is not a committed prefix point."""


def normalize_month(month: str) -> str:
    """Return ``YYYY-MM``, accepting a non-zero-padded month as a courtesy."""

    if not isinstance(month, str):
        raise ValidationError(f"month must be a string, got {type(month).__name__}")
    match = _MONTH_RE.match(month.strip())
    if not match:
        raise ValidationError(f"invalid month {month!r}; expected YYYY-MM")
    year, mon = int(match.group(1)), int(match.group(2))
    if not 1 <= mon <= 12:
        raise ValidationError(f"invalid month {month!r}")
    return f"{year:04d}-{mon:02d}"


def month_bounds(month: str) -> tuple[date, date]:
    """Return the half-open ``[start, end)`` bounds of a normalised month."""

    month = normalize_month(month)
    year, mon = (int(part) for part in month.split("-"))
    start = date(year, mon, 1)
    if mon == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, mon + 1, 1)
    return start, end


def _parse_date(value: Any, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, bool) and type(value) is not date:
        # datetime is a date subclass; only plain dates / ISO strings are legal.
        raise ValidationError(f"{field} must be an ISO date string, not datetime")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValidationError(f"{field} must be an ISO date string")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f"invalid {field} {value!r}: {exc}") from exc
    return parsed


def _validate_amount(amount: Any, op: str) -> int:
    if op == "retract":
        # ``retract`` bills as zero.  An omitted or explicit zero amount is
        # normal; a non-zero amount would be contradictory input, so reject it
        # rather than silently discarding it.
        if amount is None or amount == 0:
            return 0
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise ValidationError("retract amount must be an integer when supplied")
        raise ValidationError("retract bills as zero; amount must be omitted or 0")
    if isinstance(amount, bool) or not isinstance(amount, int):
        raise ValidationError("set amount must be an integer number of cents")
    return amount


def _canonical(op: str, meter: str, a: str, b: str, amount: int) -> str:
    return json.dumps(
        {"op": op, "meter": meter, "a": a, "b": b, "amount": amount},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _content_hash(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Facts:
    """Persistent append-only metering facts store."""

    def __init__(self, path):
        self.path = str(path)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(
            self.path, timeout=30.0, isolation_level=None, check_same_thread=False
        )
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout=30000")
        try:
            self.db.execute("PRAGMA journal_mode=WAL")
        except sqlite3.Error:
            # A read-only/odd filesystem may reject WAL; the service still works.
            pass
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS facts_events (
                seq          INTEGER PRIMARY KEY,
                event_id     TEXT NOT NULL UNIQUE,
                op           TEXT NOT NULL CHECK (op IN ('set','retract')),
                meter        TEXT NOT NULL,
                a            TEXT NOT NULL,
                b            TEXT NOT NULL,
                amount       INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                canonical    TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS facts_events_meter_seq
                ON facts_events(meter, seq);
            CREATE INDEX IF NOT EXISTS facts_events_seq
                ON facts_events(seq);
            CREATE TABLE IF NOT EXISTS facts_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self.db.execute(
            "INSERT OR IGNORE INTO facts_meta(key, value) VALUES ('schema', '1')"
        )
        self.db.commit()

    # ------------------------------------------------------------------ API

    def ingest(self, event: dict) -> dict:
        """Ingest a dict-shaped event and return its persisted record.

        Accepted keys: ``event_id``, ``meter``/``meter_id``, ``a``/``from``,
        ``b``/``to``, ``amount``, ``op`` (``set``/``retract``), and optional
        ``knowledge_seq``/``k``.  The returned record includes ``replayed``.
        """

        if not isinstance(event, dict):
            raise ValidationError("event must be a dict")
        event_id = event.get("event_id")
        meter = event.get("meter", event.get("meter_id"))
        a = event.get("a", event.get("from", event.get("valid_from")))
        b = event.get("b", event.get("to", event.get("valid_to")))
        amount = event.get("amount")
        op = event.get("op", "set")
        knowledge_seq = event.get("knowledge_seq", event.get("k"))
        record, replayed = self._ingest(
            event_id, meter, a, b, amount, op=op, knowledge_seq=knowledge_seq
        )
        out = dict(record)
        out["replayed"] = replayed
        return out

    def append(
        self,
        event_id: str,
        meter: str,
        a,
        b,
        amount: Optional[int] = None,
        *,
        op: str = "set",
        knowledge_seq: Optional[int] = None,
    ) -> int:
        """Append one event, returning its knowledge sequence.

        Replaying identical content returns the original sequence.  Reusing an
        id with different content raises :class:`EventConflict`.
        """

        record, _ = self._ingest(
            event_id, meter, a, b, amount, op=op, knowledge_seq=knowledge_seq
        )
        return record["seq"]

    def as_of(self, k: Optional[int], month: str) -> dict[str, int]:
        """Per-meter month total using only knowledge with ``seq <= k``.

        ``k=None`` means the current committed head.  Meters with any event in
        the prefix are returned, including meters whose total for *month* is
        zero (retracted or no coverage in that month).
        """

        # Accept the alternative positional order as a convenience.
        if isinstance(k, str) and isinstance(month, int):
            k, month = month, k
        month = normalize_month(month)
        cutoff = self.resolve_k(k)
        breakdown = self.breakdown(cutoff, month)
        return {meter: breakdown[meter]["total"] for meter in sorted(breakdown)}

    def breakdown(self, k: Optional[int], month: str) -> dict[str, dict]:
        """Detailed per-day resolution for a month at a knowledge cutoff.

        This is the facts side of the explanation: for every meter and day in
        the month it names the winning event (greatest covering ``seq <= k``),
        its amount, and the per-meter source-event identities.  It is read-only
        and never consults the wall clock.
        """

        if isinstance(k, str) and isinstance(month, int):
            k, month = month, k
        month = normalize_month(month)
        cutoff = self.resolve_k(k)
        start, end = month_bounds(month)

        with self._lock:
            rows = self.db.execute(
                "SELECT seq, event_id, op, meter, a, b, amount "
                "FROM facts_events WHERE seq <= ? ORDER BY seq ASC",
                (cutoff,),
            ).fetchall()

        daily: dict[str, dict[date, dict]] = {}
        known: set[str] = set()
        for row in rows:
            meter = row["meter"]
            known.add(meter)
            ev_a = date.fromisoformat(row["a"])
            ev_b = date.fromisoformat(row["b"])
            lo = ev_a if ev_a > start else start
            hi = ev_b if ev_b < end else end
            if lo >= hi:
                continue
            rec = {
                "amount": int(row["amount"]),
                "seq": int(row["seq"]),
                "event_id": row["event_id"],
                "op": row["op"],
            }
            # Rows are ascending, so later (greater seq) overwrites earlier.
            per_meter = daily.setdefault(meter, {})
            day = lo
            while day < hi:
                per_meter[day] = rec
                day += _ONE_DAY

        result: dict[str, dict] = {}
        for meter in sorted(known):
            days = daily.get(meter, {})
            total = sum(rec["amount"] for rec in days.values())
            day_map = {
                day.isoformat(): {
                    "amount": rec["amount"],
                    "seq": rec["seq"],
                    "event_id": rec["event_id"],
                    "op": rec["op"],
                }
                for day, rec in sorted(days.items())
            }
            sources: dict[int, dict] = {}
            for day, rec in sorted(days.items()):
                src = sources.get(rec["seq"])
                if src is None:
                    src = {
                        "event_id": rec["event_id"],
                        "seq": rec["seq"],
                        "op": rec["op"],
                        "meter": meter,
                        "days": [],
                    }
                    sources[rec["seq"]] = src
                src["days"].append(day.isoformat())
                # Fill canonical event fields lazily from the log below.
            if sources:
                seqs = sorted(sources)
                placeholders = ",".join("?" for _ in seqs)
                with self._lock:
                    ev_rows = self.db.execute(
                        "SELECT seq, a, b, amount FROM facts_events "
                        f"WHERE seq IN ({placeholders})",
                        seqs,
                    ).fetchall()
                for ev in ev_rows:
                    src = sources[int(ev["seq"])]
                    src["a"] = ev["a"]
                    src["b"] = ev["b"]
                    src["amount"] = int(ev["amount"])
                    src["day_count"] = len(src["days"])
            result[meter] = {
                "total": total,
                "days": day_map,
                "sources": [sources[seq] for seq in sorted(sources)],
            }
        return result

    def changes(self, after_seq: int = 0, through_seq: Optional[int] = None) -> list[dict]:
        """Committed events with ``after_seq < seq <= through_seq``.

        ``through_seq=None`` (or a value past the head) means the committed
        head.  This is the replay feed for rebuilding any historical view.
        """

        if isinstance(after_seq, bool) or not isinstance(after_seq, int):
            raise ValidationError("after_seq must be an integer")
        if after_seq < 0:
            raise ValidationError("after_seq must be >= 0")
        with self._lock:
            head = self._head_unlocked()
        if through_seq is None:
            through = head
        else:
            if isinstance(through_seq, bool) or not isinstance(through_seq, int):
                raise ValidationError("through_seq must be an integer")
            if through_seq < 0:
                raise ValidationError("through_seq must be >= 0")
            through = min(through_seq, head)

        with self._lock:
            rows = self.db.execute(
                "SELECT * FROM facts_events WHERE seq > ? AND seq <= ? ORDER BY seq ASC",
                (after_seq, through),
            ).fetchall()
        return [self._row_to_record(row) for row in rows]

    def history(
        self,
        after_seq: int = 0,
        through_seq: Optional[int] = None,
        meter: Optional[str] = None,
    ) -> list[dict]:
        """Alias for :meth:`changes`, optionally filtered to one meter."""

        records = self.changes(after_seq, through_seq)
        if meter is not None:
            records = [rec for rec in records if rec["meter"] == meter]
        return records

    def inspect(
        self,
        event_id: Optional[str] = None,
        seq: Optional[int] = None,
    ) -> Optional[dict]:
        """Return one persisted event record, or ``None`` if absent."""

        if event_id is None and seq is None:
            raise ValidationError("inspect requires event_id or seq")
        if isinstance(event_id, int) and seq is None and not isinstance(event_id, bool):
            seq, event_id = event_id, None
        with self._lock:
            if event_id is not None:
                row = self.db.execute(
                    "SELECT * FROM facts_events WHERE event_id = ?", (event_id,)
                ).fetchone()
            else:
                if isinstance(seq, bool) or not isinstance(seq, int):
                    raise ValidationError("seq must be an integer")
                row = self.db.execute(
                    "SELECT * FROM facts_events WHERE seq = ?", (seq,)
                ).fetchone()
        return None if row is None else self._row_to_record(row)

    def head(self) -> int:
        """The greatest committed knowledge sequence (0 when empty)."""

        with self._lock:
            return self._head_unlocked()

    # Alias used by callers that speak in terms of the committed prefix.
    knowledge_head = head

    def resolve_k(self, k: Optional[int]) -> int:
        """Validate that ``k`` is a committed knowledge prefix point."""

        head = self.head()
        if k is None:
            return head
        if isinstance(k, bool) or not isinstance(k, int):
            raise KnowledgeCutoffError("knowledge cutoff must be an integer")
        if k < 0 or k > head:
            raise KnowledgeCutoffError(
                f"knowledge cutoff {k} is outside committed prefix 0..{head}"
            )
        return k

    def close(self) -> None:
        with self._lock:
            self.db.close()

    # -------------------------------------------------------------- internals

    def _ingest(
        self,
        event_id,
        meter,
        a,
        b,
        amount,
        *,
        op: str,
        knowledge_seq: Optional[int],
    ) -> tuple[dict, bool]:
        if not isinstance(event_id, str) or not event_id:
            raise ValidationError("event_id must be a non-empty string")
        if not isinstance(meter, str) or not meter:
            raise ValidationError("meter must be a non-empty string")
        if not isinstance(op, str):
            raise ValidationError("op must be 'set' or 'retract'")
        op = op.strip().lower()
        if op not in ("set", "retract"):
            raise ValidationError("op must be 'set' or 'retract'")

        a_date = _parse_date(a, "a")
        b_date = _parse_date(b, "b")
        if a_date >= b_date:
            raise ValidationError("valid interval must be non-empty: a < b")
        span_days = (b_date - a_date).days
        if span_days > _MAX_RANGE_DAYS:
            raise ValidationError(
                f"valid interval spans {span_days} days; at most {_MAX_RANGE_DAYS} allowed"
            )
        months_spanned = {
            (a_date + timedelta(days=offset)).strftime("%Y-%m")
            for offset in range(span_days)
        }
        if len(months_spanned) > _MAX_MONTHS_SPANNED:
            raise ValidationError(
                "valid interval may span at most two natural months: "
                + ", ".join(sorted(months_spanned))
            )

        amount_norm = _validate_amount(amount, op)
        if knowledge_seq is not None:
            if isinstance(knowledge_seq, bool) or not isinstance(knowledge_seq, int):
                raise KnowledgeCutoffError("knowledge_seq must be an integer")
            if knowledge_seq < 1:
                raise KnowledgeCutoffError("knowledge_seq must be >= 1")

        a_iso, b_iso = a_date.isoformat(), b_date.isoformat()
        canonical = _canonical(op, meter, a_iso, b_iso, amount_norm)
        content_hash = _content_hash(canonical)

        with self._lock:
            self._begin()
            try:
                existing = self.db.execute(
                    "SELECT * FROM facts_events WHERE event_id = ?", (event_id,)
                ).fetchone()
                if existing is not None:
                    if existing["content_hash"] != content_hash:
                        raise EventConflict(
                            f"event_id {event_id!r} already exists with different content"
                        )
                    self.db.execute("COMMIT")
                    return self._row_to_record(existing), True

                next_seq = self._head_unlocked() + 1
                if knowledge_seq is not None and knowledge_seq != next_seq:
                    raise KnowledgeCutoffError(
                        f"knowledge_seq {knowledge_seq} is not the next contiguous "
                        f"prefix value {next_seq}"
                    )
                self.db.execute(
                    "INSERT INTO facts_events"
                    "(seq, event_id, op, meter, a, b, amount, content_hash, canonical)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        next_seq,
                        event_id,
                        op,
                        meter,
                        a_iso,
                        b_iso,
                        amount_norm,
                        content_hash,
                        canonical,
                    ),
                )
                self.db.execute("COMMIT")
            except BaseException:
                try:
                    self.db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

        record = {
            "seq": next_seq,
            "event_id": event_id,
            "op": op,
            "meter": meter,
            "a": a_iso,
            "b": b_iso,
            "amount": amount_norm,
            "content_hash": content_hash,
        }
        return record, False

    def _begin(self) -> None:
        # BEGIN IMMEDIATE takes the write lock up front so two appenders cannot
        # both compute the same next sequence.  busy_timeout provides the wait.
        self.db.execute("BEGIN IMMEDIATE")

    def _head_unlocked(self) -> int:
        row = self.db.execute("SELECT COALESCE(MAX(seq), 0) AS head FROM facts_events").fetchone()
        return int(row["head"])

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> dict:
        return {
            "seq": int(row["seq"]),
            "event_id": row["event_id"],
            "op": row["op"],
            "meter": row["meter"],
            "a": row["a"],
            "b": row["b"],
            "amount": int(row["amount"]),
            "content_hash": row["content_hash"],
            "canonical": row["canonical"],
        }
