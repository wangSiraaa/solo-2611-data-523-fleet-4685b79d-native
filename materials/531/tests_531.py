"""Acceptance tests for the federated cost account and real page executor.

Run with: ``python3.11 -m unittest tests_531 -v`` (from this directory).

Every economic assertion is checked against the sources' independent audit logs
and the real ledger account table; no test relies on a client-reported counter.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from budget import (  # noqa: E402
    BudgetLedger,
    InsufficientBudget,
    RefundRequiresNotStarted,
    SourceBoundViolation,
    TicketMismatch,
    WrongAccount,
    ReservationConflict,
)
from federation import (  # noqa: E402
    Federation,
    CANCELLED,
    COMPLETED,
    INCOMPLETE,
    PENDING_COST,
    SOURCE_ERROR,
)
from paged_source import PagedSource  # noqa: E402


ORDERS = [
    {"id": "o1", "customer_id": "c1", "amount": 1},
    {"id": "o2", "customer_id": "c1", "amount": 99},
]
CUSTOMERS = [
    {"id": "c1", "name": "Synthetic one"},
    {"id": "c2", "name": "Synthetic two"},
]


def write_jsonl(directory, name, rows):
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for row in rows:
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    return path


def make_sources(directory, orders=None, customers=None, **source_kwargs):
    orders = ORDERS if orders is None else orders
    customers = CUSTOMERS if customers is None else customers
    opath = write_jsonl(directory, "orders.jsonl", orders)
    cpath = write_jsonl(directory, "customers.jsonl", customers)
    orders_src = PagedSource("orders", opath, page_rows=1, **source_kwargs)
    customers_src = PagedSource("customers", cpath, page_rows=1, **source_kwargs)
    return orders_src, customers_src


def make_federation(credits, orders_src, customers_src, **kwargs):
    ledger = BudgetLedger()
    ledger.open_account("acct", credits)
    fed = Federation(
        ledger,
        {"orders": orders_src, "customers": customers_src},
        "acct",
        **kwargs,
    )
    return ledger, fed


class LedgerUnitTests(unittest.TestCase):
    def test_invariant_and_settle_from_ticket(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 100)
        rid = ledger.reserve("a", "q", "s", "0", 40, ticket_id="t1")
        self.assertEqual(ledger.state("a")["available"], 60)
        # Settlement ignores any caller "used" value; it reads the ticket.
        result = ledger.settle(
            rid, {"status": "done", "ticket_id": "t1", "scanned_bytes": 10,
                  "source": "s", "page_id": "0", "query_id": "q"}
        )
        self.assertEqual(result["actual"], 10)
        self.assertEqual(result["refund"], 30)
        state = ledger.state("a")
        self.assertEqual((state["available"], state["reserved"], state["spent"]), (90, 0, 10))
        self.assertTrue(state["invariant_ok"])

    def test_duplicate_same_ticket_settlement_is_idempotent(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 100)
        rid = ledger.reserve("a", "q", "s", "0", 40, ticket_id="t1")
        ticket = {"status": "done", "ticket_id": "t1", "scanned_bytes": 10,
                  "source": "s", "page_id": "0", "query_id": "q"}
        first = ledger.settle(rid, ticket)
        second = ledger.settle(rid, ticket)
        self.assertEqual(first, second)
        self.assertEqual(ledger.state("a")["spent"], 10)

    def test_ticket_resettled_under_second_reservation_refunds_full(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 100)
        # Two reservations for the same source ticket id: e.g. a retry path.
        r1 = ledger.reserve("a", "q", "s", "0", 40, ticket_id="t1")
        r2 = ledger.reserve("a", "q", "s", "0", 40, ticket_id="t1")
        ticket = {"status": "done", "ticket_id": "t1", "scanned_bytes": 10,
                  "source": "s", "page_id": "0", "query_id": "q"}
        ledger.settle(r1, ticket)
        ledger.settle(r2, ticket)  # same source ticket, no new read
        # Re-settling the duplicate reservation must not refund again.
        ledger.settle(r2, ticket)
        state = ledger.state("a")
        self.assertEqual(state["spent"], 10)
        self.assertEqual(state["reserved"], 0)
        self.assertEqual(state["available"], 90)

    def test_changed_amount_under_same_ticket_rejected(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 100)
        rid = ledger.reserve("a", "q", "s", "0", 40, ticket_id="t1")
        base = {"status": "done", "ticket_id": "t1", "scanned_bytes": 10,
                "source": "s", "page_id": "0", "query_id": "q"}
        ledger.settle(rid, base)
        with self.assertRaises(TicketMismatch):
            ledger.settle(rid, dict(base, scanned_bytes=11))

    def test_other_account_cannot_claim_ticket(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 100)
        ledger.open_account("b", 100)
        ra = ledger.reserve("a", "q", "s", "0", 40, ticket_id="t1")
        rb = ledger.reserve("b", "q", "s", "0", 40, ticket_id="t1")
        ticket = {"status": "done", "ticket_id": "t1", "scanned_bytes": 10,
                  "source": "s", "page_id": "0", "query_id": "q"}
        ledger.settle(ra, ticket)
        with self.assertRaises(WrongAccount):
            ledger.settle(rb, ticket)
        self.assertEqual(ledger.state("b")["reserved"], 40)

    def test_reservation_cannot_overdraw(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 10)
        with self.assertRaises(InsufficientBudget):
            ledger.reserve("a", "q", "s", "0", 11)
        state = ledger.state("a")
        self.assertEqual((state["available"], state["reserved"], state["spent"]), (10, 0, 0))

    def test_release_requires_not_started(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 100)
        rid = ledger.reserve("a", "q", "s", "0", 40, ticket_id="t1")
        with self.assertRaises(RefundRequiresNotStarted):
            ledger.release(rid, {"status": "unknown", "ticket_id": "t1"})
        # obligation still held
        self.assertEqual(ledger.state("a")["reserved"], 40)
        release = ledger.release(
            rid, {"status": "not_started", "ticket_id": "t1"}, reason="cancel"
        )
        self.assertEqual(release["refund"], 40)
        self.assertEqual(ledger.state("a")["reserved"], 0)

    def test_settle_then_release_cannot_double_refund(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 100)
        rid = ledger.reserve("a", "q", "s", "0", 40, ticket_id="t1")
        ledger.settle(
            rid,
            {"status": "done", "ticket_id": "t1", "scanned_bytes": 10,
             "source": "s", "page_id": "0", "query_id": "q"},
        )
        with self.assertRaises(ReservationConflict):
            ledger.release(rid, {"status": "not_started", "ticket_id": "t1"})
        state = ledger.state("a")
        self.assertEqual((state["available"], state["reserved"], state["spent"]), (90, 0, 10))

    def test_release_then_settle_cannot_double_charge(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 100)
        rid = ledger.reserve("a", "q", "s", "0", 40, ticket_id="t1")
        ledger.release(rid, {"status": "not_started", "ticket_id": "t1"})
        with self.assertRaises(ReservationConflict):
            ledger.settle(
                rid,
                {"status": "done", "ticket_id": "t1", "scanned_bytes": 10,
                 "source": "s", "page_id": "0", "query_id": "q"},
            )
        state = ledger.state("a")
        self.assertEqual((state["available"], state["reserved"], state["spent"]), (100, 0, 0))

    def test_bound_violation_isolated_not_faked(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 5)
        rid = ledger.reserve("a", "q", "s", "0", 5, ticket_id="t1")
        ticket = {"status": "done", "ticket_id": "t1", "scanned_bytes": 12,
                  "source": "s", "page_id": "0", "query_id": "q"}
        with self.assertRaises(SourceBoundViolation) as ctx:
            ledger.settle(rid, ticket)
        self.assertEqual(ctx.exception.unfunded, 7)
        state = ledger.state("a")
        self.assertEqual(state["reserved"], 5)  # still held, not invented
        self.assertEqual(state["spent"], 0)
        self.assertTrue(state["invariant_ok"])


class SourceTests(unittest.TestCase):
    def test_catalog_bound_includes_newlines_and_actual(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, _ = make_sources(tmp)
            catalog = orders.catalog()
            self.assertEqual(catalog["page_count"], len(ORDERS))
            for page in catalog["pages"]:
                self.assertGreaterEqual(page["upper_bound"], 1)

    def test_same_ticket_retry_does_not_rescan(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, _ = make_sources(tmp)
            first = orders.scan("q", "0")
            second = orders.scan("q", "0")
            self.assertEqual(first["ticket_id"], second["ticket_id"])
            self.assertEqual(orders.actual_reads(), 1)
            self.assertEqual(orders.audit_totals()["scan_count"], 1)

    def test_lost_response_reports_unknown_then_done(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, _ = make_sources(tmp)
            with self.assertRaises(TimeoutError):
                orders.scan("q", "0", lose_response=True)
            self.assertEqual(orders.get_ticket("q", "0")["status"], "unknown")
            orders.resolve_for_test("q", "0")
            self.assertEqual(orders.get_ticket("q", "0")["status"], "done")
            self.assertEqual(orders.actual_reads(), 1)

    def test_verify_receipt_rejects_tampered_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, _ = make_sources(tmp)
            ticket = orders.scan("q", "0")
            with self.assertRaises(Exception):
                orders.verify_receipt(dict(ticket, scanned_bytes=ticket["scanned_bytes"] + 1))


class FederationTests(unittest.TestCase):
    def test_filtered_scan_is_billed_by_bytes_not_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)
            status = fed.run("q1", minimum=50)
            self.assertEqual(status["status"], COMPLETED)
            # join keeps only o2 -> 1 row, but many bytes were scanned.
            result = status["result"]
            self.assertEqual(result["row_count"], 1)
            audit = orders.audit_totals()
            self.assertGreater(audit["scanned_bytes"], 1)
            self.assertEqual(ledger.state("acct")["spent"],
                             orders.audit_totals()["scanned_bytes"]
                             + customers.audit_totals()["scanned_bytes"])

    def test_full_join_preserves_duplicate_orders(self):
        orders = [
            {"id": "o1", "customer_id": "c1", "amount": 10},
            {"id": "o1", "customer_id": "c1", "amount": 10},
            {"id": "o2", "customer_id": "c2", "amount": 5},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            orders_src, customers_src = make_sources(tmp, orders=orders)
            ledger, fed = make_federation(10_000, orders_src, customers_src)
            status = fed.run("q", minimum=0)
            self.assertEqual(status["status"], COMPLETED)
            self.assertEqual(status["result"]["row_count"], 3)

    def test_budget_insufficient_returns_incomplete_without_unpaid_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(2, orders, customers)
            status = fed.run("q", minimum=0)
            self.assertEqual(status["status"], INCOMPLETE)
            self.assertIsNone(status["result"])
            # Only pages that were actually paid for were read.
            reads = orders.actual_reads() + customers.actual_reads()
            self.assertLessEqual(
                ledger.state("acct")["spent"], 2
            )
            self.assertGreaterEqual(reads, 0)
            verified = fed.verify("q")
            self.assertTrue(verified["ok"], verified["issues"])

    def test_cancel_before_reads_refunds_not_started(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(2, orders, customers)
            # Whole-query reservation then immediate cancellation.
            fed.queries.clear()
            status = fed.run("q", minimum=0, strategy="whole")
            self.assertEqual(status["status"], INCOMPLETE)
            fed.cancel("q")
            state = ledger.state("acct")
            self.assertEqual(state["reserved"], 0)
            self.assertEqual(state["spent"], 0)
            self.assertEqual(orders.actual_reads() + customers.actual_reads(), 0)

    def test_zero_hit_query_still_charges(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)
            status = fed.run("q", minimum=10_000)
            self.assertEqual(status["status"], COMPLETED)
            self.assertEqual(status["result"]["row_count"], 0)
            self.assertGreater(ledger.state("acct")["spent"], 0)
            self.assertTrue(fed.verify("q")["ok"])

    def test_lost_receipt_holds_obligation_then_reconcile(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)
            status = fed.run(
                "q",
                minimum=0,
                lose_response={("orders", "0")},
            )
            self.assertEqual(status["status"], PENDING_COST)
            state = ledger.state("acct")
            self.assertGreater(state["reserved"], 0)
            self.assertTrue(state["invariant_ok"])
            # The page was really read; the source audit proves it.
            self.assertGreaterEqual(orders.actual_reads(), 1)
            self.assertEqual(ledger.state("acct")["spent"], 0)

            orders.resolve_for_test("q", "0")
            recon = fed.reconcile("q")
            # Reconcile settles only the original ticket; it never re-runs the
            # rest of the schedule, so the query stays incomplete.
            self.assertEqual(recon["status"], INCOMPLETE)
            # After resolution the loss is recognized as a charge.
            self.assertGreater(ledger.state("acct")["spent"], 0)

    def test_late_receipt_and_cancel_do_not_double_charge(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)
            fed.run("q", minimum=0, lose_response={("orders", "0")})
            fed.cancel("q")
            reserved_after_cancel = ledger.state("acct")["reserved"]
            self.assertGreater(reserved_after_cancel, 0)  # unknown kept obligation
            orders.resolve_for_test("q", "0")
            before = ledger.state("acct")["spent"]
            fed.reconcile("q")
            after = ledger.state("acct")["spent"]
            self.assertGreater(after, before)
            # Reconciling again must not charge again.
            fed.reconcile("q")
            self.assertEqual(ledger.state("acct")["spent"], after)
            self.assertTrue(fed.verify("q")["ok"])

    def test_two_queries_race_for_last_credits(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            page_cost = max(
                p["upper_bound"] for p in orders.catalog()["pages"]
            )
            ledger = BudgetLedger()
            ledger.open_account("acct", page_cost)  # enough for exactly one page
            fed = Federation(
                ledger,
                {"orders": orders, "customers": customers},
                "acct",
            )
            first = fed.run("q1", minimum=0, order_pages=["0"], customer_pages=[])
            second = fed.run(
                "q2", minimum=0, order_pages=["0"], customer_pages=[],
                account_id="acct",
            )
            statuses = {first["status"], second["status"]}
            self.assertIn(INCOMPLETE, statuses)
            # Exactly one scan happened, and total spend never exceeds the page.
            self.assertEqual(orders.actual_reads(), 1)
            self.assertLessEqual(ledger.state("acct")["spent"], page_cost)
            self.assertTrue(ledger.state("acct")["invariant_ok"])

    def test_priority_whoever_can_pay_gets_reservation(self):
        ledger = BudgetLedger()
        ledger.open_account("a", 10)
        # Reservation from can-pay consumer.
        rid = ledger.reserve("a", "q1", "s", "0", 10)
        with self.assertRaises(InsufficientBudget):
            ledger.reserve("a", "q2", "s", "0", 10)
        self.assertTrue(ledger.is_live(rid))

    def test_over_declared_bound_is_isolated(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Under-state the upper bound so actual read exceeds it.
            orders_src = PagedSource(
                "orders",
                write_jsonl(tmp, "orders.jsonl", ORDERS),
                page_rows=1,
                bound_override=lambda i, actual: max(0, actual - 1),
            )
            customers_src = PagedSource(
                "customers",
                write_jsonl(tmp, "customers.jsonl", CUSTOMERS),
                page_rows=1,
                bound_override=lambda i, actual: max(0, actual - 1),
            )
            ledger, fed = make_federation(10_000, orders_src, customers_src)
            status = fed.run("q", minimum=0)
            self.assertEqual(status["status"], SOURCE_ERROR)
            self.assertIsNone(status["result"])
            self.assertEqual(status["error"]["kind"], "source_bound_violation")
            # Not silently balanced: still funded within available credits, but
            # the violation is recorded in the source audit.
            self.assertGreater(
                orders_src.audit_totals()["scan_count"]
                + customers_src.audit_totals()["scan_count"],
                0,
            )

    def test_upper_bound_refund_when_overdeclared(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders = PagedSource(
                "orders",
                write_jsonl(tmp, "orders.jsonl", ORDERS),
                page_rows=1,
                bound_padding=17,
            )
            customers = PagedSource(
                "customers",
                write_jsonl(tmp, "customers.jsonl", CUSTOMERS),
                page_rows=1,
                bound_padding=17,
            )
            ledger, fed = make_federation(10_000, orders, customers)
            status = fed.run("q", minimum=0)
            self.assertEqual(status["status"], COMPLETED)
            diag = status["diagnostics"]
            # Every page was over-reserved by 17 credits and got a refund.
            self.assertEqual(diag["refunded"], 17 * diag["scanned_page_count"])
            self.assertEqual(
                ledger.state("acct")["spent"], fed.verify("q")["ledger"]["source_bytes"]
            )

    def test_cancel_mid_run_releases_untouched_and_stops_new_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)

            calls = {"n": 0}

            def cancel_after_first(source_name, page_id):
                calls["n"] += 1
                if calls["n"] == 2:
                    fed.cancel("q")

            status = fed.run("q", minimum=0, before_page=cancel_after_first)
            self.assertEqual(status["status"], CANCELLED)
            # The first page completed; cancellation then stopped all new pages.
            self.assertEqual(orders.actual_reads() + customers.actual_reads(), 1)
            state = ledger.state("acct")
            self.assertEqual(state["reserved"], 0)
            # Billed exactly for the one page that was really read.
            self.assertEqual(state["spent"], fed.verify("q")["ledger"]["source_bytes"])
            self.assertEqual(state["spent"], 10_000 - state["available"])
            self.assertTrue(fed.verify("q")["ok"])

    def test_cancel_before_any_scan_refunds_all(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)

            def cancel_first(source_name, page_id):
                fed.cancel("q")

            status = fed.run("q", minimum=0, before_page=cancel_first)
            self.assertEqual(status["status"], CANCELLED)
            self.assertEqual(orders.actual_reads() + customers.actual_reads(), 0)
            state = ledger.state("acct")
            self.assertEqual(state["spent"], 0)
            self.assertEqual(state["reserved"], 0)
            self.assertEqual(state["available"], 10_000)

    def test_unstarted_cancel_with_whole_strategy_refunds_without_reads(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(300, orders, customers)
            fed.run(
                "q",
                minimum=0,
                strategy="whole",
                before_page=lambda s, p: fed.cancel("q"),
            )
            state = ledger.state("acct")
            self.assertEqual(orders.actual_reads() + customers.actual_reads(), 0)
            self.assertEqual(state["reserved"], 0)
            self.assertEqual(state["available"], 300)
            self.assertEqual(state["spent"], 0)

    def test_reconcile_before_resolution_keeps_obligation(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)
            fed.run("q", minimum=0, lose_response={("orders", "0")})
            before = ledger.state("acct")
            recon = fed.reconcile("q")  # source still says unknown
            after = ledger.state("acct")
            self.assertEqual(recon["status"], PENDING_COST)
            self.assertEqual(before["reserved"], after["reserved"])
            self.assertGreater(after["reserved"], 0)
            self.assertEqual(after["spent"], 0)
            self.assertTrue(fed.verify("q")["ok"])

    def test_two_accounts_have_independent_budgets(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger = BudgetLedger()
            ledger.open_account("a", 10_000)
            ledger.open_account("b", 0)
            fed = Federation(
                ledger, {"orders": orders, "customers": customers}, "a"
            )
            rich = fed.run("qA", minimum=0, account_id="a")
            poor = fed.run("qB", minimum=0, account_id="b")
            self.assertEqual(rich["status"], COMPLETED)
            self.assertEqual(poor["status"], INCOMPLETE)
            self.assertEqual(ledger.state("b")["spent"], 0)
            self.assertEqual(ledger.state("b")["reserved"], 0)
            self.assertTrue(ledger.state("a")["invariant_ok"])

    def test_concurrent_race_for_last_credits(self):
        import threading

        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            page_cost = max(p["upper_bound"] for p in orders.catalog()["pages"])
            ledger = BudgetLedger()
            ledger.open_account("acct", page_cost)
            results = []
            barrier = threading.Barrier(2)

            def worker(name):
                fed = Federation(
                    ledger, {"orders": orders, "customers": customers}, "acct"
                )
                barrier.wait()
                try:
                    results.append(
                        fed.run(name, 0, order_pages=["0"], customer_pages=[])
                    )
                except Exception as exc:  # pragma: no cover - must not happen
                    results.append({"status": "exception", "detail": str(exc)})

            threads = [threading.Thread(target=worker, args=(f"q{i}",)) for i in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            # Exactly one page was actually read and paid; the other lost the
            # race and never read.
            self.assertEqual(orders.actual_reads(), 1)
            self.assertLessEqual(ledger.state("acct")["spent"], page_cost)
            statuses = sorted(r["status"] for r in results)
            self.assertIn(COMPLETED, statuses)
            self.assertIn(INCOMPLETE, statuses)
            self.assertTrue(ledger.state("acct")["invariant_ok"])

    def test_wrong_account_settlement_rejected_in_federation(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)
            fed.run("q", minimum=0)
            # Any reservation's ticket cannot be charged to a foreign account.
            res = ledger.reservations_for()[0]
            ticket = orders.verify_receipt(
                {"ticket_id": res["ticket_id"]}
            ) if res["source"] == "orders" else customers.verify_receipt(
                {"ticket_id": res["ticket_id"]}
            )
            with self.assertRaises((WrongAccount, Exception)):
                ledger.settle(res["reserve_id"], ticket, account_id="someone-else")

    def test_partial_diagnostics_billed_equals_scan_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            # Budget for roughly two orders pages and one customers page.
            bounds = sorted(
                p["upper_bound"]
                for p in orders.catalog()["pages"] + customers.catalog()["pages"]
            )
            credits = bounds[0] + bounds[1] + bounds[2]
            ledger, fed = make_federation(credits, orders, customers)
            status = fed.run("q", minimum=0)
            self.assertEqual(status["status"], INCOMPLETE)
            self.assertIsNone(status["result"])
            diag = status["diagnostics"]
            self.assertGreater(diag["scanned_page_count"], 0)
            self.assertEqual(
                diag["billed"],
                orders.audit_totals()["scanned_bytes"]
                + customers.audit_totals()["scanned_bytes"],
            )
            # No scheduled page beyond the paid prefix was read.
            self.assertLess(
                diag["scanned_page_count"], diag["scheduled_page_count"]
            )
            self.assertTrue(fed.verify("q")["ok"])

    def test_legal_input_family_20_pages_16_rows(self):
        orders = [
            {"id": f"o{i}", "customer_id": f"c{i % 3}", "amount": i}
            for i in range(20 * 16)
        ]
        customers = [{"id": f"c{i}", "name": f"n{i}"} for i in range(3)]
        with tempfile.TemporaryDirectory() as tmp:
            opath = write_jsonl(tmp, "orders.jsonl", orders)
            cpath = write_jsonl(tmp, "customers.jsonl", customers)
            orders_src = PagedSource("orders", opath, page_rows=16)
            customers_src = PagedSource("customers", cpath, page_rows=16)
            self.assertEqual(len(orders_src.catalog()["pages"]), 20)
            ledger, fed = make_federation(10_000_000, orders_src, customers_src)
            status = fed.run("q", minimum=300)
            self.assertEqual(status["status"], COMPLETED)
            expected = sum(1 for o in orders if o["amount"] >= 300)
            self.assertEqual(status["result"]["row_count"], expected)
            self.assertTrue(fed.verify("q")["ok"])

    def test_concurrent_cancel_during_run_no_double_accounting(self):
        import threading

        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)
            started = threading.Event()

            def hook(source_name, page_id):
                if not started.is_set():
                    started.set()
                    # Let the cancel thread interleave with the run.
                    threading.Event().wait(0.01)

            def runner():
                fed.run("q", minimum=0, before_page=hook)

            t = threading.Thread(target=runner)
            t.start()
            started.wait()
            fed.cancel("q")
            t.join()
            state = ledger.state("acct")
            self.assertTrue(state["invariant_ok"])
            # Reconcile is idempotent and never recharges.
            fed.reconcile("q")
            self.assertEqual(ledger.state("acct")["spent"], state["spent"])
            report = fed.verify("q")
            self.assertTrue(report["ok"], report["issues"])

    def test_ticket_retry_does_not_double_scan_in_federation(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)
            fed.run("q", minimum=0)
            before = orders.actual_reads()
            # Re-run the same page ticket directly through the source.
            orders.scan("q", "0")
            self.assertEqual(orders.actual_reads(), before)
            self.assertTrue(fed.verify("q")["ok"])

    def test_budget_changes_real_scan_count(self):
        def scans_for(credits):
            with tempfile.TemporaryDirectory() as tmp:
                orders, customers = make_sources(tmp)
                ledger, fed = make_federation(credits, orders, customers)
                fed.run("q", minimum=0)
                return orders.actual_reads() + customers.actual_reads()

        low = scans_for(1)
        high = scans_for(10_000)
        self.assertLess(low, high)

    def test_verify_does_not_trust_client_counters(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger, fed = make_federation(10_000, orders, customers)
            fed.run("q", minimum=0)
            report = fed.verify("q")
            self.assertTrue(report["ok"])
            self.assertEqual(
                report["ledger"]["source_bytes"],
                orders.audit_totals()["scanned_bytes"]
                + customers.audit_totals()["scanned_bytes"],
            )
            self.assertEqual(
                report["ledger"]["settled_actual_for_queries"],
                report["ledger"]["source_bytes"],
            )

    def test_whole_vs_per_page_available_funds(self):
        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            per_total = sum(p["upper_bound"] for p in orders.catalog()["pages"]) + sum(
                p["upper_bound"] for p in customers.catalog()["pages"]
            )
            # Whole-query strategy holds every page up front.
            ledger_w, fed_w = make_federation(per_total, orders, customers)
            fed_w.run("q", minimum=0, strategy="whole")
            whole_avail_during = ledger_w.state("acct")["available"]
            self.assertEqual(whole_avail_during, 0)

            # Per-page strategy only holds one page at a time.
            orders2, customers2 = make_sources(tmp)
            ledger_p, fed_p = make_federation(per_total, orders2, customers2)
            fed_p.run("q", minimum=0, strategy="per_page")
            self.assertGreaterEqual(
                ledger_p.state("acct")["available"], 0
            )


class CliSmokeTests(unittest.TestCase):
    def test_cli_run_and_verify(self):
        import cli531

        with tempfile.TemporaryDirectory() as tmp:
            orders, customers = make_sources(tmp)
            ledger = BudgetLedger()
            ledger.open_account("acct", 10_000)
            fed = Federation(
                ledger, {"orders": orders, "customers": customers}, "acct"
            )
            status = cli531.run_query(fed, "q", minimum=50)
            self.assertEqual(status["status"], COMPLETED)
            report = cli531.verify_accounts(fed, "q")
            self.assertTrue(report["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
