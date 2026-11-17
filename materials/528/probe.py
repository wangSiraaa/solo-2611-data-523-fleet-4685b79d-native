"""End-to-end scenario probe.

Exercises the facts ledger and settlement service against every case named in
the task: the 100 -> 120 supplement booked once, a cross-day partial
correction, a negative refund, retract-then-reset, an interval spanning two
natural months, a reused event id with different content, persisted idempotency
across a reopen, and two connections reconciling concurrently.

Echoing the starting probe, this prints JSON.  It also runs the independent
verifier (``verify.py``) which rebuilds amounts from a plain per-day dict, so
the services are not proving their own arithmetic.

Run:  python probe.py
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
from pathlib import Path

from facts import Facts, EventConflict, KnowledgeCutoffError, ValidationError
from settlement import Settlement
import verify


def _ok(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _raw_bill(bills_path: Path, month: str) -> bytes:
    conn = sqlite3.connect(str(bills_path))
    try:
        row = conn.execute(
            "SELECT content_json FROM bills WHERE month = ?", (month,)
        ).fetchone()
        return row[0]
    finally:
        conn.close()


def run() -> dict:
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        facts_path = td / "facts.db"
        bills_path = td / "bills.db"
        facts = Facts(facts_path)
        settle = Settlement(bills_path, facts)
        log: dict = {}

        # ---- 2026-01: base, seal, and a no-change first reconciliation ----
        seq_base = facts.append("base", "m1", "2026-01-01", "2026-01-02", 100)
        bill = settle.seal("2026-01", seq_base)
        first = settle.reconcile("2026-01", seq_base)
        _ok(bill["total"] == 100, "seal should freeze 100")
        _ok(first["delta"] == 0 and first["status"] == "no_change",
            "first reconcile against fresh seal must be zero")

        old_bill = settle.statement("2026-01")["bill"]
        old_raw_bill = _raw_bill(bills_path, "2026-01")
        old_as_of = facts.as_of(seq_base, "2026-01")
        log["seal"] = {"bill_id": bill["bill_id"], "k": bill["k"], "total": bill["total"]}
        log["first_reconcile"] = {"delta": first["delta"], "status": first["status"]}

        # ---- 100 -> 120: supplement exactly once -------------------------
        seq_fix = facts.append("correct", "m1", "2026-01-01", "2026-01-02", 120)
        rec_fix = settle.reconcile("2026-01", seq_fix)
        rec_fix_again = settle.reconcile("2026-01", seq_fix)
        _ok(rec_fix["delta"] == 20 and rec_fix["adjustment_id"] is not None,
            "correction must book +20")
        _ok(rec_fix_again["replayed"] is True and rec_fix_again["posted_this_round"] == 0,
            "replaying the same knowledge must not book a second +20")
        _ok(settle.posted("2026-01") == 20, "posted must be 20 after replay")
        log["supplement"] = {
            "first_delta": rec_fix["delta"],
            "second_replayed": rec_fix_again["replayed"],
            "second_posted": rec_fix_again["posted_this_round"],
            "posted_total": settle.posted("2026-01"),
        }

        # ---- retract (zero) then reset: negative refund then re-charge ----
        seq_retract = facts.append(
            "retract", "m1", "2026-01-01", "2026-01-02", op="retract"
        )
        rec_retract = settle.reconcile("2026-01", seq_retract)
        _ok(rec_retract["delta"] == -120, "retract must refund -120")
        _ok(facts.as_of(seq_retract, "2026-01")["m1"] == 0,
            "retract must bill as zero")
        seq_reset = facts.append("reset", "m1", "2026-01-01", "2026-01-02", 150)
        rec_reset = settle.reconcile("2026-01", seq_reset)
        _ok(rec_reset["delta"] == 150, "reset must re-book +150")
        _ok(settle.posted("2026-01") == 50, "m1 net adjustment should be 50")
        log["retract_reset"] = {
            "retract_delta": rec_retract["delta"],
            "reset_delta": rec_reset["delta"],
            "posted_after_reset": settle.posted("2026-01"),
        }

        # ---- cross-day partial coverage on m2 ----------------------------
        seq_m2_base = facts.append("m2base", "m2", "2026-01-01", "2026-01-06", 10)
        seq_m2_fix = facts.append("m2fix", "m2", "2026-01-03", "2026-01-06", 30)
        _ok(facts.as_of(seq_m2_fix, "2026-01")["m2"] == 110,
            "partial coverage: 2 days @10 + 3 days @30 = 110")
        rec_m2 = settle.reconcile("2026-01", seq_m2_fix)
        _ok(rec_m2["delta"] == 110, "m2 delta should be 110")
        log["partial_coverage"] = {"m2_target": 110, "delta": rec_m2["delta"]}

        # ---- interval spanning two natural months ------------------------
        seq_m3 = facts.append("m3base", "m3", "2026-01-31", "2026-02-02", 5)
        rec_m3 = settle.reconcile("2026-01", seq_m3)
        _ok(rec_m3["delta"] == 5, "Jan share of m3 should be one day @5")
        feb_bill = settle.seal("2026-02", seq_m3)
        feb_rec = settle.reconcile("2026-02", seq_m3)
        _ok(feb_bill["total"] == 5, "Feb share of m3 should be one day @5")
        _ok(feb_rec["delta"] == 0, "Feb is fresh, no change")
        log["two_month_span"] = {
            "jan_delta": rec_m3["delta"],
            "feb_sealed_total": feb_bill["total"],
        }

        # ---- same event id, different content is rejected ----------------
        head_before = facts.head()
        conflict = False
        try:
            facts.append("base", "m1", "2026-01-01", "2026-01-02", 999)
        except EventConflict:
            conflict = True
        _ok(conflict, "reused id with different content must be rejected")
        _ok(facts.head() == head_before, "rejected event must not consume a sequence")
        replay_seq = facts.append("base", "m1", "2026-01-01", "2026-01-02", 100)
        _ok(replay_seq == seq_base, "identical replay must return the original seq")
        log["idempotency"] = {
            "conflict_rejected": conflict,
            "head_unchanged": facts.head() == head_before,
            "replay_seq": replay_seq,
        }

        # ---- illegal events leave no half-written state -----------------
        head_before_illegal = facts.head()
        rejected: list[str] = []
        illegal_cases = [
            ("bad_empty_range", "2026-01-02", "2026-01-02", 5, "set"),
            ("bad_float", "2026-01-01", "2026-01-02", 1.5, "set"),
            ("bad_three_months", "2026-01-31", "2026-04-01", 5, "set"),
            ("bad_retract_amount", "2026-01-01", "2026-01-02", 7, "retract"),
        ]
        for event_id, a, b, amount, op in illegal_cases:
            try:
                facts.append(event_id, "bad", a, b, amount, op=op)
            except (ValidationError, KnowledgeCutoffError):
                rejected.append(event_id)
        _ok(len(rejected) == len(illegal_cases), "every illegal event must be rejected")
        _ok(facts.head() == head_before_illegal,
            "illegal events must not consume a knowledge sequence")
        _ok(facts.inspect("bad_float") is None, "no partial row for illegal event")
        log["illegal_events"] = {"rejected": rejected, "head_unchanged": True}

        # ---- history / changes / inspect reconstruct the log -------------
        all_events = facts.changes(0)
        _ok([row["seq"] for row in all_events] == list(range(1, facts.head() + 1)),
            "changes must expose the contiguous committed prefix")
        window = facts.changes(seq_base, seq_fix)
        _ok([row["event_id"] for row in window] == ["correct"],
            "changes window must select only the requested seq range")
        _ok(facts.inspect("base")["seq"] == seq_base, "inspect by event id")
        _ok(facts.inspect(seq=seq_fix)["event_id"] == "correct", "inspect by seq")
        log["history_api"] = {
            "changes_all": len(all_events),
            "window_event_ids": [row["event_id"] for row in window],
        }

        # ---- historical as_of is stable; old bill bytes unchanged -------
        jan_history = {
            "k1": facts.as_of(seq_base, "2026-01"),
            "k_fix": facts.as_of(seq_fix, "2026-01"),
            "k_retract": facts.as_of(seq_retract, "2026-01"),
            "k_now": facts.as_of(None, "2026-01"),
        }
        _ok(jan_history["k1"]["m1"] == 100, "as_of at base must still say 100")
        _ok(jan_history["k_fix"]["m1"] == 120, "as_of at correction must say 120")
        _ok(jan_history["k_retract"]["m1"] == 0, "as_of at retract must say 0")
        _ok("m2" not in jan_history["k1"], "m2 must not exist at base knowledge")

        now_bill = settle.statement("2026-01")["bill"]
        _ok(now_bill["content_hash"] == old_bill["content_hash"],
            "bill receipt hash stable across statements")
        raw_bill_after = _raw_bill(bills_path, "2026-01")
        _ok(raw_bill_after == old_raw_bill,
            "sealed bill bytes must not change after later knowledge")
        log["history"] = {key: value for key, value in jan_history.items()}

        # ---- stale knowledge is refused, seal is immutable --------------
        stale = settle.reconcile("2026-01", seq_base)
        _ok(stale["status"] == "stale" and stale["delta"] == 0,
            "older cutoff must return STALE with no new charge")
        reseal = settle.seal("2026-01", seq_base)
        _ok(reseal["replayed"] is True, "same-k seal replays")
        reseal_later = settle.seal("2026-01", seq_fix)
        _ok(
            reseal_later["replayed"] is True
            and reseal_later["sealed_k"] == seq_base
            and reseal_later["total"] == 100,
            "later-k seal must replay the original immutable decision",
        )
        seal_conflict = reseal_later["sealed_k"] != seq_fix
        _ok(seal_conflict, "later-k seal must not overwrite the issued bill")
        bill_update_blocked = False
        raw = sqlite3.connect(str(bills_path))
        try:
            raw.execute("UPDATE bills SET sealed_k = sealed_k WHERE month='2026-01'")
        except sqlite3.DatabaseError:
            bill_update_blocked = True
        finally:
            raw.close()
        _ok(bill_update_blocked, "storage layer must forbid bill updates")
        log["stale_and_immutable"] = {
            "stale_status": stale["status"],
            "reseal_replayed": reseal["replayed"],
            "reseal_conflict": seal_conflict,
            "bill_update_blocked": bill_update_blocked,
        }

        jan_statement = settle.statement("2026-01")
        log["jan_statement"] = {
            "sealed_k": jan_statement["sealed_k"],
            "last_k": jan_statement["last_k"],
            "total_initial": jan_statement["total_initial"],
            "total_adjustments": jan_statement["total_adjustments"],
            "total_cumulative": jan_statement["total_cumulative"],
            "balances": jan_statement["balances"],
        }
        _ok(jan_statement["total_cumulative"] == 265,
            f"Jan cumulative should be 265, got {jan_statement['total_cumulative']}")

        # ---- two connections reconcile the same supplement concurrently --
        con_base = facts.append("cbase", "c1", "2026-03-01", "2026-03-02", 100)
        con_bill = settle.seal("2026-03", con_base)
        _ok(con_bill["total"] == 100, "March seal should be 100")
        facts.append("cfix1", "c1", "2026-03-01", "2026-03-02", 120)
        con_k = facts.append("cfix2", "c1", "2026-03-01", "2026-03-02", 120)

        f_a = Facts(facts_path)
        s_a = Settlement(bills_path, f_a)
        f_b = Facts(facts_path)
        s_b = Settlement(bills_path, f_b)
        barrier = threading.Barrier(2)
        results: list[dict] = []
        errors: list[str] = []

        def worker(service: Settlement) -> None:
            try:
                barrier.wait(timeout=10)
                results.append(service.reconcile("2026-03", con_k))
            except BaseException as exc:  # surface thread failures in the main flow
                errors.append(repr(exc))

        threads = [threading.Thread(target=worker, args=(s_a,)), threading.Thread(target=worker, args=(s_b,))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        f_a.close()
        s_a.close()
        f_b.close()
        s_b.close()
        _ok(not errors, f"concurrent reconcile raised: {errors}")
        _ok(len(results) == 2, "both reconcilers must return a receipt")
        posted_rounds = sorted(receipt["posted_this_round"] for receipt in results)
        _ok(posted_rounds == [0, 20],
            f"exactly one reconciler books +20, got {posted_rounds}")
        total_march = settle.posted("2026-03")
        _ok(total_march == 20, f"March posted must be 20, got {total_march}")
        log["concurrent_reconcile"] = {
            "receipts": [
                {
                    "status": receipt["status"],
                    "delta": receipt["delta"],
                    "posted_this_round": receipt["posted_this_round"],
                    "replayed": receipt.get("replayed", False),
                }
                for receipt in results
            ],
            "posted_total": total_march,
        }
        stale_march = settle.reconcile("2026-03", con_base)
        _ok(stale_march["status"] == "stale", "older March cutoff must be STALE")
        log["concurrent_reconcile"]["stale_after"] = stale_march["status"]

        # ---- persistence: reopen and replay ------------------------------
        before = {
            "head": facts.head(),
            "as_of": facts.as_of(con_k, "2026-01"),
            "statement": settle.statement("2026-01"),
        }
        facts.close()
        settle.close()
        facts = Facts(facts_path)
        settle = Settlement(bills_path, facts)
        after = {
            "head": facts.head(),
            "as_of": facts.as_of(con_k, "2026-01"),
            "statement": settle.statement("2026-01"),
        }
        _ok(before["head"] == after["head"], "head must survive reopen")
        _ok(before["as_of"] == after["as_of"], "as_of must survive reopen")
        _ok(
            before["statement"]["bill"]["content_hash"]
            == after["statement"]["bill"]["content_hash"],
            "bill hash must survive reopen",
        )
        replay_after_reopen = facts.append("base", "m1", "2026-01-01", "2026-01-02", 100)
        _ok(replay_after_reopen == seq_base, "idempotent replay must survive reopen")
        log["persistence"] = {
            "head": after["head"],
            "replay_seq_after_reopen": replay_after_reopen,
        }

        # ---- independent verification (per-day dict oracle) -------------
        report = verify.audit(str(bills_path), str(facts_path))
        log["verify"] = report
        _ok(report["ok"], f"independent verification failed: {report['failures']}")

        facts.close()
        settle.close()
        return log


if __name__ == "__main__":
    outcome = run()
    print(json.dumps(outcome, indent=2, ensure_ascii=False))
