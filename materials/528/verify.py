"""Independent verifier for the facts ledger and settlement service.

This module deliberately does **not** import ``facts`` or ``settlement``.  It
re-opens the SQLite files, reads the raw event log, and rebuilds per-day
knowledge with a plain ``{(meter, date): record}`` dictionary.  The dictionary
is the oracle for every amount it checks, so the services' own aggregation code
is never used to prove itself.

Checks performed per sealed month:

* each bill line equals the oracle target at the bill's ``sealed_k`` (the old
  bill was not overwritten by later knowledge);
* the bill's stored content hash matches its stored canonical bytes;
* every committed adjustment line's ``target``, ``initial_bill``,
  ``prior_adjustments`` and ``delta`` match an oracle replay at that
  adjustment's cutoff;
* initial bill + all committed adjustment deltas per meter equal the oracle
  target at the month's last settlement cutoff (so no double posting and no
  uncounted negative adjustment);
* decision cutoffs move monotonically forward;
* sealed bills reject UPDATE through the storage layer.

Run directly: ``python verify.py <settlement.db> <facts.db>``.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
from datetime import date, timedelta
from typing import Iterable, Optional

__all__ = ["IndependentOracle", "audit", "read_facts_rows", "month_bounds"]

_ONE_DAY = timedelta(days=1)


def month_bounds(month: str) -> tuple[date, date]:
    year, mon = (int(part) for part in month.split("-"))
    start = date(year, mon, 1)
    end = date(year + 1, 1, 1) if mon == 12 else date(year, mon + 1, 1)
    return start, end


class IndependentOracle:
    """Per-day dict replay of the raw facts log."""

    def __init__(self, rows: Iterable[dict]):
        # Enforce the log ordering explicitly so a mis-ordered input cannot
        # silently change resolution semantics.
        self.rows = sorted(
            (dict(row) for row in rows), key=lambda row: int(row["seq"])
        )

    def daily(self, k: int, month: str) -> dict[tuple[str, date], dict]:
        """Winning record for every (meter, day) in ``month`` at cutoff ``k``."""

        start, end = month_bounds(month)
        winner: dict[tuple[str, date], dict] = {}
        for row in self.rows:
            seq = int(row["seq"])
            if seq > k:
                break
            ev_a = date.fromisoformat(row["a"])
            ev_b = date.fromisoformat(row["b"])
            lo = max(ev_a, start)
            hi = min(ev_b, end)
            if lo >= hi:
                continue
            rec = {
                "amount": int(row["amount"]),
                "seq": seq,
                "event_id": row["event_id"],
                "op": row["op"],
            }
            day = lo
            while day < hi:
                # Ascending replay: last writer for a day is the greatest seq.
                winner[(row["meter"], day)] = rec
                day += _ONE_DAY
        return winner

    def as_of(self, k: int, month: str) -> dict[str, int]:
        """Per-meter month totals at cutoff ``k`` (meters known in prefix)."""

        known = {
            row["meter"] for row in self.rows if int(row["seq"]) <= k
        }
        totals = {meter: 0 for meter in known}
        for (meter, _day), rec in self.daily(k, month).items():
            totals[meter] += rec["amount"]
        return totals


def read_facts_rows(facts_path: str) -> list[dict]:
    """Re-open the facts database and read the append-only log."""

    conn = sqlite3.connect(str(facts_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT seq, event_id, op, meter, a, b, amount "
            "FROM facts_events ORDER BY seq ASC"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _read_settlement(settle_path: str) -> dict:
    conn = sqlite3.connect(str(settle_path))
    conn.row_factory = sqlite3.Row
    try:
        bills = [dict(row) for row in conn.execute("SELECT * FROM bills").fetchall()]
        bill_lines = [
            dict(row)
            for row in conn.execute("SELECT * FROM bill_lines").fetchall()
        ]
        decisions = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM decisions ORDER BY decision_id ASC"
            ).fetchall()
        ]
        adjustments = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM adjustments ORDER BY adjustment_id ASC"
            ).fetchall()
        ]
        adjustment_lines = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM adjustment_lines ORDER BY adjustment_id, meter ASC"
            ).fetchall()
        ]
        states = [dict(row) for row in conn.execute("SELECT * FROM month_state").fetchall()]
    finally:
        conn.close()
    return {
        "bills": bills,
        "bill_lines": bill_lines,
        "decisions": decisions,
        "adjustments": adjustments,
        "adjustment_lines": adjustment_lines,
        "states": states,
    }


def _attempt_bill_update(settle_path: str) -> bool:
    """Return True when the storage layer refuses to mutate a sealed bill."""

    conn = sqlite3.connect(str(settle_path))
    try:
        conn.execute("BEGIN")
        try:
            conn.execute("UPDATE bills SET sealed_k = sealed_k")
        except sqlite3.DatabaseError:
            conn.execute("ROLLBACK")
            return True
        conn.execute("ROLLBACK")
        return False
    finally:
        conn.close()


def audit(settle_path: str, facts_path: str) -> dict:
    """Re-open both databases and independently verify every settled amount."""

    failures: list[str] = []
    rows = read_facts_rows(facts_path)
    oracle = IndependentOracle(rows)
    data = _read_settlement(settle_path)

    lines_by_bill: dict[int, list[dict]] = {}
    for line in data["bill_lines"]:
        lines_by_bill.setdefault(int(line["bill_id"]), []).append(line)
    adj_lines_by_id: dict[int, list[dict]] = {}
    for line in data["adjustment_lines"]:
        adj_lines_by_id.setdefault(int(line["adjustment_id"]), []).append(line)
    state_by_month = {row["month"]: row for row in data["states"]}

    checks = 0
    months_checked = []

    for bill in data["bills"]:
        month = bill["month"]
        sealed_k = int(bill["sealed_k"])
        bill_id = int(bill["bill_id"])
        months_checked.append(month)

        stored_hash = hashlib.sha256(bill["content_json"].encode("utf-8")).hexdigest()
        checks += 1
        if stored_hash != bill["content_hash"]:
            failures.append(
                f"{month}: bill {bill_id} content hash does not match stored bytes"
            )

        initial = {line["meter"]: int(line["amount"]) for line in lines_by_bill.get(bill_id, [])}
        oracle_sealed = oracle.as_of(sealed_k, month)
        for meter in sorted(set(initial) | set(oracle_sealed)):
            checks += 1
            if int(initial.get(meter, 0)) != int(oracle_sealed.get(meter, 0)):
                failures.append(
                    f"{month}: sealed bill {meter}={initial.get(meter, 0)} "
                    f"but oracle@{sealed_k}={oracle_sealed.get(meter, 0)}"
                )

        # Per-meter replay of committed adjustments in commit order.  Track the
        # money actually posted (initial + sum of deltas) independently of the
        # targets stored on each line, so the final identity cannot be satisfied
        # merely by copying targets around.
        cumulative = {meter: int(amount) for meter, amount in initial.items()}
        for adj in data["adjustments"]:
            if adj["month"] != month:
                continue
            adj_id = int(adj["adjustment_id"])
            adj_k = int(adj["k"])
            oracle_k = oracle.as_of(adj_k, month)
            line_sum = 0
            for line in adj_lines_by_id.get(adj_id, []):
                meter = line["meter"]
                checks += 1
                expected_initial = int(initial.get(meter, 0))
                expected_prior = int(cumulative.get(meter, 0)) - expected_initial
                expected_target = int(oracle_k.get(meter, 0))
                expected_delta = expected_target - (expected_initial + expected_prior)
                if int(line["initial_bill"]) != expected_initial:
                    failures.append(
                        f"{month}: adj {adj_id} {meter} initial_bill drift"
                    )
                if int(line["prior_adjustments"]) != expected_prior:
                    failures.append(
                        f"{month}: adj {adj_id} {meter} prior_adjustments drift"
                    )
                if int(line["target"]) != expected_target:
                    failures.append(
                        f"{month}: adj {adj_id} {meter} target != oracle@{adj_k}"
                    )
                if int(line["delta"]) != expected_delta:
                    failures.append(
                        f"{month}: adj {adj_id} {meter} delta {line['delta']} "
                        f"!= {expected_delta}"
                    )
                line_sum += int(line["delta"])
                cumulative[meter] = int(cumulative.get(meter, 0)) + int(line["delta"])
            checks += 1
            if int(adj["delta_total"]) != line_sum:
                failures.append(f"{month}: adj {adj_id} total is not the line sum")

        # Settled total per meter must equal the target at the last cutoff.
        if month in state_by_month:
            last_k = int(state_by_month[month]["last_k"])
            oracle_last = oracle.as_of(last_k, month)
            for meter in sorted(set(cumulative) | set(oracle_last)):
                checks += 1
                settled = int(cumulative.get(meter, 0))
                target = int(oracle_last.get(meter, 0))
                if settled != target:
                    failures.append(
                        f"{month}: settled {meter}={settled} != oracle@{last_k}={target}"
                    )
            total_settled = sum(cumulative.values())
            total_target = sum(oracle_last.values())
            checks += 1
            if total_settled != total_target:
                failures.append(
                    f"{month}: total settled {total_settled} != target {total_target}"
                )

    # Decisions must advance knowledge monotonically per month.
    last_k_by_month: dict[str, int] = {}
    for decision in data["decisions"]:
        month = decision["month"]
        k = int(decision["k"])
        checks += 1
        if month in last_k_by_month and k < last_k_by_month[month]:
            failures.append(f"{month}: decision k moved backwards to {k}")
        last_k_by_month[month] = k

    # Version and last decision pointer agree with the stored decision rows.
    for month, state in state_by_month.items():
        month_decisions = [d for d in data["decisions"] if d["month"] == month]
        checks += 1
        if int(state["version"]) != len(month_decisions):
            failures.append(
                f"{month}: version {state['version']} != {len(month_decisions)} decisions"
            )
        if month_decisions:
            checks += 1
            if int(state["last_decision_id"]) != int(month_decisions[-1]["decision_id"]):
                failures.append(f"{month}: last_decision_id is not the latest decision")

    checks += 1
    if data["bills"] and not _attempt_bill_update(settle_path):
        failures.append("sealed bills accepted an UPDATE; immutability is broken")

    return {
        "ok": not failures,
        "checks": checks,
        "months": sorted(set(months_checked)),
        "bills": len(data["bills"]),
        "adjustments": len(data["adjustments"]),
        "decisions": len(data["decisions"]),
        "failures": failures,
    }


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: python verify.py <settlement.db> <facts.db>", file=sys.stderr)
        return 2
    report = audit(argv[1], argv[2])
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
