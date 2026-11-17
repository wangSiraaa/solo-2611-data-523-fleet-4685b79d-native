"""Probe / demo: real metered federated joins against the fixture files.

Run: ``python3.11 probe.py``

The output is a demonstration of the implemented behaviour, verified against
the sources' independent audit logs and the real ledger account table.  It is
not the acceptance suite; that lives in ``tests_531.py``.
"""

from __future__ import annotations

import json
import os
import tempfile

from budget import BudgetLedger
from federation import Federation
from paged_source import PagedSource

HERE = os.path.dirname(os.path.abspath(__file__))


def sources(page_orders=os.path.join(HERE, "orders.jsonl"),
            page_customers=os.path.join(HERE, "customers.jsonl"), **kw):
    orders = PagedSource("orders", page_orders, page_rows=1, **kw)
    customers = PagedSource("customers", page_customers, page_rows=1, **kw)
    return orders, customers


def fed(credits, orders, customers, **kw):
    ledger = BudgetLedger()
    ledger.open_account("acct", credits)
    return ledger, Federation(ledger, {"orders": orders, "customers": customers}, "acct", **kw)


def scenario_normal():
    orders, customers = sources()
    ledger, f = fed(100_000, orders, customers)
    status = f.run("q.filter", minimum=50)
    return {
        "status": status["status"],
        "result_rows": status["result"]["row_count"],
        "original_bytes_scanned": (
            orders.audit_totals()["scanned_bytes"]
            + customers.audit_totals()["scanned_bytes"]
        ),
        "billed": ledger.state("acct")["spent"],
        "rows_are_not_bytes": ledger.state("acct")["spent"] != status["result"]["row_count"],
    }


def scenario_budget_changes_scans():
    out = {}
    for credits in (1, 30, 10_000):
        orders, customers = sources()
        ledger, f = fed(credits, orders, customers)
        status = f.run("q", minimum=0)
        out[str(credits)] = {
            "status": status["status"],
            "real_scans": orders.actual_reads() + customers.actual_reads(),
            "spent": ledger.state("acct")["spent"],
        }
    return out


def scenario_two_queries_one_purse():
    orders, customers = sources()
    page_cost = max(p["upper_bound"] for p in orders.catalog()["pages"])
    ledger = BudgetLedger()
    ledger.open_account("acct", page_cost)
    f = Federation(ledger, {"orders": orders, "customers": customers}, "acct")
    a = f.run("qA", 0, order_pages=["0"], customer_pages=[])
    b = f.run("qB", 0, order_pages=["0"], customer_pages=[])
    return {
        "statuses": [a["status"], b["status"]],
        "real_scans": orders.actual_reads(),
        "state": ledger.state("acct"),
    }


def scenario_zero_hits():
    orders, customers = sources()
    ledger, f = fed(100_000, orders, customers)
    status = f.run("q", minimum=10_000)
    return {
        "status": status["status"],
        "result_rows": status["result"]["row_count"],
        "billed": ledger.state("acct")["spent"],
    }


def scenario_lost_then_cancel():
    orders, customers = sources()
    ledger, f = fed(100_000, orders, customers)
    f.run("q", minimum=0, lose_response={("orders", "0")})
    held = ledger.state("acct")["reserved"]
    f.cancel("q")
    held_after_cancel = ledger.state("acct")["reserved"]
    orders.resolve_for_test("q", "0")
    f.reconcile("q")
    return {
        "held_when_unknown": held,
        "held_after_cancel": held_after_cancel,
        "spent_after_reconcile": ledger.state("acct")["spent"],
        "verify_ok": f.verify("q")["ok"],
    }


def scenario_bound_violation():
    with tempfile.TemporaryDirectory() as tmp:
        opath = os.path.join(tmp, "o.jsonl")
        cpath = os.path.join(tmp, "c.jsonl")
        open(opath, "w").write('{"id":"o1","customer_id":"c1","amount":1}\n')
        open(cpath, "w").write('{"id":"c1","name":"x"}\n')
        orders = PagedSource("orders", opath, bound_override=lambda i, n: max(0, n - 3))
        customers = PagedSource("customers", cpath, bound_override=lambda i, n: max(0, n - 3))
        ledger, f = fed(100_000, orders, customers)
        status = f.run("q", minimum=0)
        return {
            "status": status["status"],
            "error_kind": (status["error"] or {}).get("kind"),
            "verify_ok": f.verify("q")["ok"],
            "verify_issues": [i["kind"] for i in f.verify("q")["issues"]],
        }


def main():
    report = {
        "normal_filtered_join": scenario_normal(),
        "budget_changes_real_scans": scenario_budget_changes_scans(),
        "two_queries_one_purse": scenario_two_queries_one_purse(),
        "zero_hit_still_billed": scenario_zero_hits(),
        "lost_receipt_then_cancel_then_reconcile": scenario_lost_then_cancel(),
        "source_bound_violation_isolated": scenario_bound_violation(),
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
