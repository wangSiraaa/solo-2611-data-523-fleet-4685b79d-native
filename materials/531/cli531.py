"""Small command-line front end exposing run/cancel/reconcile/account/status/verify.

The CLI builds two real ``PagedSource`` objects from the fixture files and a
single ``BudgetLedger`` account, so it is a working (if minimal) driver rather
than a mock.  Programmatic use goes through ``Federation`` directly.

Because a bare CLI invocation is a fresh process (and therefore has no prior
in-memory query/source state), the verbs that normally act on an existing query
(``cancel``, ``reconcile``, ``query-status``, ``verify``) accept an optional
``--run`` header so you can chain them in one process, e.g.::

    python3.11 cli531.py run q --minimum 50 --then cancel,account,verify

``run`` first, then each ``--then`` step executes in order and the outputs are
printed as a list.  A missing query in a standalone invocation reports a JSON
error instead of a traceback.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from budget import BudgetLedger
from federation import Federation
from paged_source import PagedSource

HERE = os.path.dirname(os.path.abspath(__file__))


def build(sources_dir=HERE, credits=10_000, page_rows=1, bound_padding=0):
    orders = PagedSource(
        "orders",
        os.path.join(sources_dir, "orders.jsonl"),
        page_rows=page_rows,
        bound_padding=bound_padding,
    )
    customers = PagedSource(
        "customers",
        os.path.join(sources_dir, "customers.jsonl"),
        page_rows=page_rows,
        bound_padding=bound_padding,
    )
    ledger = BudgetLedger()
    ledger.open_account("acct", credits)
    federation = Federation(
        ledger, {"orders": orders, "customers": customers}, "acct"
    )
    return ledger, federation, orders, customers


def run_query(federation, query_id, minimum, *, strategy="per_page", **kw):
    return federation.run(query_id, minimum, strategy=strategy, **kw)


def cancel_query(federation, query_id):
    return federation.cancel(query_id)


def reconcile_query(federation, query_id):
    return federation.reconcile(query_id)


def account_report(federation, account_id=None):
    return federation.account(account_id)


def query_status(federation, query_id):
    return federation.query_status(query_id)


def verify_accounts(federation, query_id=None):
    return federation.verify(query_id)


def _dispatch(verb, federation, query_id, args):
    if verb == "account":
        return account_report(federation)
    if verb == "verify":
        return verify_accounts(federation, query_id)
    if verb == "query-status":
        return query_status(federation, query_id)
    if verb == "cancel":
        return cancel_query(federation, query_id)
    if verb == "reconcile":
        return reconcile_query(federation, query_id)
    if verb == "run":
        return run_query(
            federation, query_id, args.minimum, strategy=args.strategy
        )
    raise ValueError(verb)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Federated cost-account demo")
    parser.add_argument(
        "verb",
        choices=["run", "cancel", "reconcile", "account", "query-status", "verify"],
    )
    parser.add_argument("query_id", nargs="?")
    parser.add_argument("--minimum", type=int, default=0)
    parser.add_argument("--credits", type=int, default=10_000)
    parser.add_argument("--strategy", choices=["per_page", "whole"], default="per_page")
    parser.add_argument("--sources", default=HERE)
    parser.add_argument("--page-rows", type=int, default=1)
    parser.add_argument("--bound-padding", type=int, default=0)
    parser.add_argument(
        "--then",
        default="",
        help="comma-separated follow-up steps run in the same process",
    )
    args = parser.parse_args(argv)

    ledger, federation, _orders, _customers = build(
        sources_dir=args.sources,
        credits=args.credits,
        page_rows=args.page_rows,
        bound_padding=args.bound_padding,
    )

    outputs = []
    try:
        outputs.append(_dispatch(args.verb, federation, args.query_id, args))
        for step in [s.strip() for s in args.then.split(",") if s.strip()]:
            outputs.append(_dispatch(step, federation, args.query_id, args))
    except KeyError as exc:
        outputs.append({"error": "unknown_query", "detail": str(exc)})

    payload = outputs[0] if len(outputs) == 1 else outputs
    json.dump(payload, sys.stdout, indent=2, ensure_ascii=False, default=str)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
