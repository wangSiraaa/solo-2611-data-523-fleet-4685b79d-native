"""Federated execution over two real paged sources.

A query reads a fixed schedule of pages from the orders source and the
customers source, then performs the fixed inner join
``order.customer_id == customer.id`` with ``order.amount >= minimum``.  Duplicate
orders are preserved.

Economic rules enforced here (the ledger enforces the bookkeeping, the source
enforces the read):

* A page is never read before a reservation that can pay its declared upper
  bound exists for that account/query/source/page.
* Charging is based on the source ticket (``scanned_bytes``), never on the
  number of output rows and never on a client-supplied figure.
* If the budget cannot pay for the next page, execution stops and reports
  ``incomplete`` with diagnostics for the part that did complete.  It never
  reads an unpaid page first and then reports an overrun.
* Cancellation stops new pages.  Pages already read are settled; a lost reply
  (``unknown``) keeps its full reservation held until ``reconcile`` resolves
  the original ticket.  A ``not_started`` receipt is the only thing that allows
  a refund.
* Source errors are classified using the source ticket, not assumed to be free.
"""

from __future__ import annotations

import copy
import threading

from budget import (
    InsufficientBudget,
    LedgerError,
    RefundRequiresNotStarted,
    ReservationConflict,
    SourceBoundViolation,
    TicketMismatch,
    WrongAccount,
)
from paged_source import SourceError


# Query terminal states.  ``pending_cost`` is terminal until reconciled.
RUNNING = "running"
COMPLETED = "completed"
INCOMPLETE = "incomplete"
CANCELLED = "cancelled"
SOURCE_ERROR = "source_error"
PENDING_COST = "pending_cost"

TERMINAL = {COMPLETED, INCOMPLETE, CANCELLED, SOURCE_ERROR, PENDING_COST}


class Federation:
    """Executes federated queries and records the reserve->scan->settle chain."""

    def __init__(
        self,
        ledger,
        sources,
        account_id,
        *,
        orders_source=None,
        customers_source=None,
        strategy="per_page",
    ):
        self.ledger = ledger
        self.account_id = account_id
        if isinstance(sources, dict):
            self.sources = dict(sources)
        else:
            self.sources = {s.name: s for s in sources}
        self.orders_source = orders_source or self._infer("order")
        self.customers_source = customers_source or self._infer("customer")
        if self.orders_source == self.customers_source:
            raise ValueError("orders and customers must be distinct sources")
        for name in (self.orders_source, self.customers_source):
            if name not in self.sources:
                raise ValueError(f"missing source {name!r}")
        if strategy not in {"per_page", "whole"}:
            raise ValueError("strategy must be 'per_page' or 'whole'")
        self.strategy = strategy
        self.queries = {}
        self._lock = threading.RLock()
        # Make the ledger canonicalize tickets against the sources themselves,
        # so even a direct ledger caller cannot fabricate "expected" bytes.
        for name, source in self.sources.items():
            if hasattr(source, "verify_receipt"):
                self.ledger.register_source(name, source.verify_receipt)

    # ------------------------------------------------------------------ setup
    def _infer(self, needle):
        for name in self.sources:
            if needle in name.lower():
                return name
        raise ValueError(f"cannot infer {needle} source from {list(self.sources)}")

    def _source(self, name):
        return self.sources[name]

    # -------------------------------------------------------------------- run
    def run(
        self,
        query_id,
        minimum,
        *,
        strategy=None,
        account_id=None,
        lose_response=None,
        order_pages=None,
        customer_pages=None,
        before_page=None,
    ):
        """Execute one federated query.

        ``lose_response`` may be a bool, a set of ``(source, page_id)`` pairs,
        or ``callable(source_name, page_id) -> bool`` to simulate a read whose
        receipt is lost.
        """
        strategy = strategy or self.strategy
        account = account_id or self.account_id
        with self._lock:
            if query_id in self.queries:
                raise ValueError(f"query {query_id!r} already exists")
            orders = self._source(self.orders_source)
            customers = self._source(self.customers_source)
            page_lists = {
                orders.name: [
                    str(p)
                    for p in (
                        orders.page_ids() if order_pages is None else order_pages
                    )
                ],
                customers.name: [
                    str(p)
                    for p in (
                        customers.page_ids()
                        if customer_pages is None
                        else customer_pages
                    )
                ],
            }
            query = {
                "query_id": query_id,
                "account": account,
                "minimum": minimum,
                "strategy": strategy,
                "status": RUNNING,
                "sources": {
                    orders.name: orders.snapshot,
                    customers.name: customers.snapshot,
                },
                "order_source": orders.name,
                "customer_source": customers.name,
                "page_lists": page_lists,
                "reservations": {},  # (source, page) -> reserve_id
                "pages": {},  # (source, page) -> page record
                "scan_log": [],
                "rows": {orders.name: {}, customers.name: {}},  # page -> rows
                "cancel_requested": False,
                "result": None,
                "diagnostics": None,
                "error": None,
            }
            self.queries[query_id] = query
            schedule = self._schedule(page_lists, orders.name, customers.name)

        # Reserve the whole schedule up front when asked; per-page reserve is
        # done just before each read.
        if strategy == "whole":
            if not self._reserve_whole(query, account, schedule, orders, customers):
                with self._lock:
                    self._close_open_reservations(query, account, reason="budget")
                    self._finalize_run(query)
                return self.query_status(query_id)

        for source_name, page_id in schedule:
            with self._lock:
                q = self.queries[query_id]
                if q["cancel_requested"] or q["status"] != RUNNING:
                    break
            if before_page is not None:
                before_page(source_name, page_id)
            keep_going = self._run_page(
                query_id,
                source_name,
                page_id,
                account,
                strategy,
                lose_response,
                query,
            )
            if not keep_going:
                break

        with self._lock:
            q = self.queries[query_id]
            if q["status"] in {INCOMPLETE, SOURCE_ERROR, PENDING_COST}:
                # Any page that was reserved but never read should not keep its
                # funds hostage: release it only when the source says
                # ``not_started``.  Read or unknown pages keep their obligation.
                self._close_open_reservations(q, account, reason="run_end")
            self._finalize_run(q)
            return self.query_status(query_id)

    def _schedule(self, page_lists, orders_name, customers_name):
        """Round-robin the two sources so partial runs touch both sides."""
        o = [(orders_name, p) for p in page_lists[orders_name]]
        c = [(customers_name, p) for p in page_lists[customers_name]]
        out = []
        for i in range(max(len(o), len(c))):
            if i < len(o):
                out.append(o[i])
            if i < len(c):
                out.append(c[i])
        return out

    def _reserve_whole(self, query, account, schedule, orders, customers):
        requests = []
        for source_name, page_id in schedule:
            src = self._source(source_name)
            requests.append(
                {
                    "source": source_name,
                    "page_id": page_id,
                    "amount": self._page_bound(src, page_id),
                    "ticket_id": src.ticket_id(query["query_id"], page_id),
                }
            )
        try:
            reserve_ids = self.ledger.reserve_many(
                account, query["query_id"], requests
            )
        except InsufficientBudget as exc:
            self._set_incomplete(query, reason="budget_insufficient", detail=str(exc))
            return False
        for (source_name, page_id), rid in zip(schedule, reserve_ids):
            query["reservations"][(source_name, page_id)] = rid
            query["pages"][(source_name, page_id)] = self._new_page(
                source_name, page_id, rid
            )
        return True

    def _run_page(
        self, query_id, source_name, page_id, account, strategy, lose_response, query
    ):
        with self._lock:
            q = self.queries[query_id]
            if q["cancel_requested"] or q["status"] != RUNNING:
                return False

            key = (source_name, page_id)
            source = self._source(source_name)
            reserve_id = q["reservations"].get(key)
            if reserve_id is None:
                # per-page strategy: reserve exactly this page now.
                try:
                    reserve_id = self.ledger.reserve(
                        account,
                        query_id,
                        source_name,
                        page_id,
                        self._page_bound(source, page_id),
                        ticket_id=source.ticket_id(query_id, page_id),
                    )
                except InsufficientBudget as exc:
                    self._set_incomplete(
                        q, reason="budget_insufficient", detail=str(exc)
                    )
                    return False
                q["reservations"][key] = reserve_id
                q["pages"][key] = self._new_page(source_name, page_id, reserve_id)
            rec = q["pages"][key]

            # The read may only start once the reservation is live.
            self.ledger.begin_scan(reserve_id, account)
            rec["state"] = "scanning"
            lose = self._should_lose(lose_response, source_name, page_id)
            call = {
                "seq": len(q["scan_log"]) + 1,
                "reserve_id": reserve_id,
                "query_id": query_id,
                "source": source_name,
                "page_id": page_id,
                "scan_called": True,
                "ticket_id": source.ticket_id(query_id, page_id),
                "scanned_bytes": None,
                "settlement": None,
                "status": "scanning",
            }
            q["scan_log"].append(call)

            ticket = None
            error = None
            try:
                ticket = source.scan(query_id, page_id, lose_response=lose)
            except TimeoutError as exc:
                error = ("timeout", str(exc))
            except Exception as exc:  # source error
                error = ("source_error", f"{type(exc).__name__}: {exc}")

            # Whatever the response, consult the source ticket; an exception is
            # not evidence that the page was free.
            evidence = self._safe_get_ticket(source, query_id, page_id)
            if ticket is None and evidence and evidence.get("status") == "done":
                ticket = evidence

            if ticket is not None:
                try:
                    settlement = self.ledger.settle(
                        reserve_id,
                        ticket,
                        account_id=account,
                        verifier=source.verify_receipt,
                    )
                    self._record_settlement(q, rec, call, ticket, settlement)
                    return True
                except SourceBoundViolation as exc:
                    rec["state"] = "quarantined"
                    rec["ticket_id"] = ticket.get("ticket_id")
                    rec["violation"] = str(exc)
                    call["status"] = "quarantined"
                    call["ticket_id"] = ticket.get("ticket_id")
                    call["scanned_bytes"] = ticket.get("scanned_bytes")
                    q["error"] = {
                        "kind": "source_bound_violation",
                        "detail": str(exc),
                        "source": source_name,
                        "page_id": page_id,
                    }
                    q["status"] = SOURCE_ERROR
                    return False
                except (
                    TicketMismatch,
                    WrongAccount,
                    ReservationConflict,
                    SourceError,
                    LedgerError,
                ) as exc:
                    q["error"] = {
                        "kind": "settlement_rejected",
                        "detail": str(exc),
                        "source": source_name,
                        "page_id": page_id,
                    }
                    q["status"] = SOURCE_ERROR
                    return False

            # No usable ticket yet.
            status = (evidence or {}).get("status")
            if status == "unknown":
                # Keep the full obligation.  Do not refund, do not charge yet.
                rec["state"] = "unknown"
                rec["ticket_id"] = (evidence or {}).get("ticket_id")
                call["status"] = "unknown"
                call["ticket_id"] = (evidence or {}).get("ticket_id")
                q["status"] = PENDING_COST
                if error:
                    q["error"] = {"kind": error[0], "detail": error[1]}
                # A pending query stops issuing new pages.
                return False

            if status == "not_started":
                # The source confirms nothing was read, so a refund is honest.
                try:
                    release = self.ledger.release(
                        reserve_id,
                        evidence,
                        account_id=account,
                        reason=(error[0] if error else "not_started"),
                    )
                    rec["state"] = "released"
                    rec["release"] = release
                    call["status"] = "released"
                    call["ticket_id"] = (evidence or {}).get("ticket_id")
                except (RefundRequiresNotStarted, ReservationConflict) as exc:
                    rec["state"] = "error"
                    q["error"] = {
                        "kind": "release_rejected",
                        "detail": str(exc),
                        "source": source_name,
                        "page_id": page_id,
                    }
                q["status"] = SOURCE_ERROR
                if error:
                    q["error"] = {"kind": error[0], "detail": error[1]}
                return False

            # Truly unclassifiable: hold the reservation and report it.
            rec["state"] = "unknown"
            call["status"] = "unknown"
            q["status"] = PENDING_COST
            q["error"] = {
                "kind": error[0] if error else "unclassified",
                "detail": error[1] if error else "no ticket evidence",
            }
            return False

    # --------------------------------------------------------------- finalize
    def _finalize_run(self, q):
        if q["status"] == PENDING_COST:
            q["diagnostics"] = self._diagnostics(q)
            return
        if q["status"] != RUNNING:
            q["diagnostics"] = self._diagnostics(q)
            return
        if q["cancel_requested"]:
            q["status"] = CANCELLED
            q["diagnostics"] = self._diagnostics(q)
            return
        # All scheduled pages must be settled to publish a complete result.
        states = [rec["state"] for rec in q["pages"].values()]
        if len(q["pages"]) == len(self._all_schedule(q)) and all(
            s == "settled" for s in states
        ):
            q["status"] = COMPLETED
            q["result"] = self._join(q)
            q["diagnostics"] = self._diagnostics(q)
        elif any(s == "unknown" for s in states):
            q["status"] = PENDING_COST
            q["diagnostics"] = self._diagnostics(q)
        else:
            # Not every page was attempted (should only happen on cancel/error).
            q["status"] = CANCELLED if q["cancel_requested"] else INCOMPLETE
            q["diagnostics"] = self._diagnostics(q)

    def _all_schedule(self, q):
        return self._schedule(
            q["page_lists"], q["order_source"], q["customer_source"]
        )

    def _set_incomplete(self, q, reason, detail):
        q["status"] = INCOMPLETE
        q["error"] = {"kind": reason, "detail": detail}

    # ------------------------------------------------------------------- join
    def _join(self, q):
        order_rows = []
        for page_rows in q["rows"][q["order_source"]].values():
            order_rows.extend(page_rows)
        customer_rows = []
        for page_rows in q["rows"][q["customer_source"]].values():
            customer_rows.extend(page_rows)

        by_id = {}
        for customer in customer_rows:
            by_id.setdefault(customer.get("id"), []).append(customer)

        minimum = q["minimum"]
        out = []
        for order in order_rows:
            if order.get("amount") is None or order.get("amount") < minimum:
                continue
            for customer in by_id.get(order.get("customer_id"), []):
                out.append(
                    {
                        "order_id": order.get("id"),
                        "customer_id": order.get("customer_id"),
                        "amount": order.get("amount"),
                        "customer_name": customer.get("name"),
                        "order": copy.deepcopy(order),
                        "customer": copy.deepcopy(customer),
                    }
                )
        return {"rows": out, "row_count": len(out)}

    # ---------------------------------------------------------------- cancel
    def cancel(self, query_id, *, account_id=None):
        """Stop issuing new pages and bring existing obligations to a terminal state.

        Releases only reservations whose source says ``not_started``.  Pages
        already read are settled; unknown pages keep their obligation held.
        Idempotent and safe against a concurrent completion.
        """
        with self._lock:
            q = self._queries(query_id)
            account = account_id or q["account"]
            if q["status"] == COMPLETED:
                # Completion already settled everything; nothing to refund.
                return self.query_status(query_id)
            if q["cancel_requested"] and q["status"] in {
                CANCELLED,
                SOURCE_ERROR,
                PENDING_COST,
            }:
                return self.query_status(query_id)

            q["cancel_requested"] = True
            self._close_open_reservations(q, account, reason="cancel")

            pending = any(
                rec["state"] == "unknown" for rec in q["pages"].values()
            )
            q["status"] = PENDING_COST if pending else CANCELLED
            q["diagnostics"] = self._diagnostics(q)
            return self.query_status(query_id)

    def _close_open_reservations(self, q, account, reason):
        """For every non-terminal reservation, ask the source and act on fact."""
        for key, rec in list(q["pages"].items()):
            if rec["state"] in {"settled", "released", "quarantined"}:
                continue
            source_name, page_id = key
            source = self._source(source_name)
            evidence = self._safe_get_ticket(source, q["query_id"], page_id)
            status = (evidence or {}).get("status")
            reserve_id = rec["reserve_id"]
            ledger_state = self.ledger.reservation(reserve_id)["state"]
            if ledger_state == "scanning" and status == "not_started":
                # A read is in flight.  A ``not_started`` snapshot taken now
                # would race the read; never refund under a live scan.  Leave
                # it for the read's own settle path.
                continue
            if status == "done":
                try:
                    settlement = self.ledger.settle(
                        reserve_id,
                        evidence,
                        account_id=account,
                        verifier=source.verify_receipt,
                    )
                    self._record_settlement(
                        q, rec, self._find_call(q, reserve_id), evidence, settlement
                    )
                except SourceBoundViolation as exc:
                    rec["state"] = "quarantined"
                    rec["ticket_id"] = evidence.get("ticket_id")
                    rec["violation"] = str(exc)
                    q["error"] = {"kind": "source_bound_violation", "detail": str(exc)}
                except (
                    TicketMismatch,
                    WrongAccount,
                    ReservationConflict,
                    SourceError,
                    LedgerError,
                ) as exc:
                    rec["state"] = "error"
                    q["error"] = {"kind": "settlement_rejected", "detail": str(exc)}
            elif status == "not_started":
                try:
                    rec["release"] = self.ledger.release(
                        reserve_id, evidence, account_id=account, reason=reason
                    )
                    rec["state"] = "released"
                except (RefundRequiresNotStarted, ReservationConflict) as exc:
                    rec["state"] = "error"
                    q["error"] = {"kind": "release_rejected", "detail": str(exc)}
            else:  # unknown
                rec["state"] = "unknown"

    # -------------------------------------------------------------- reconcile
    def reconcile(self, query_id, *, account_id=None):
        """settle or release the original tickets; never re-run the query.

        Only pages that already have a reservation are touched.  No page is
        scanned, and the join is not recomputed unless the query becomes fully
        settled and was only pending cost.
        """
        with self._lock:
            q = self._queries(query_id)
            account = account_id or q["account"]
            self._close_open_reservations(q, account, reason="reconcile")

            states = [rec["state"] for rec in q["pages"].values()]
            unknown = any(s == "unknown" for s in states)
            all_settled = bool(q["pages"]) and all(s == "settled" for s in states)
            scheduled = len(self._all_schedule(q))
            all_pages_present = len(q["pages"]) == scheduled

            if unknown:
                q["status"] = PENDING_COST
            elif all_settled and all_pages_present and not q["cancel_requested"]:
                # It was only waiting for cost resolution.
                q["status"] = COMPLETED
                q["result"] = self._join(q)
            elif q["cancel_requested"]:
                q["status"] = CANCELLED
            elif any(s in {"quarantined", "error"} for s in states):
                q["status"] = SOURCE_ERROR
            else:
                q["status"] = INCOMPLETE
            q["diagnostics"] = self._diagnostics(q)
            return self.query_status(query_id)

    # ----------------------------------------------------------------- account
    def account(self, account_id=None):
        return self.ledger.state(account_id or self.account_id)

    def accounts(self):
        return self.ledger.accounts()

    # ------------------------------------------------------------ query status
    def query_status(self, query_id):
        with self._lock:
            q = self._queries(query_id)
            return {
                "query_id": q["query_id"],
                "account": q["account"],
                "status": q["status"],
                "strategy": q["strategy"],
                "minimum": q["minimum"],
                "sources": copy.deepcopy(q["sources"]),
                "order_source": q["order_source"],
                "customer_source": q["customer_source"],
                "result": copy.deepcopy(q["result"])
                if q["status"] == COMPLETED
                else None,
                "diagnostics": copy.deepcopy(q["diagnostics"]),
                "error": copy.deepcopy(q["error"]),
                "cancel_requested": q["cancel_requested"],
            }

    # convenience alias for the "query-status" verb.
    queryStatus = query_status

    # -------------------------------------------------------------------- verify
    def verify(self, query_id=None):
        """Cross-check source audits against the real ledger accounts.

        The acceptance check does not trust caller-side counters.  It walks the
        sources' independent audit logs and the ledger's reservations.  An
        audit entry with no settled reservation for the same ticket is an
        "unpaid read" and a hard failure.
        """
        with self._lock:
            queries = (
                [self._queries(query_id)]
                if query_id is not None
                else list(self.queries.values())
            )
            issues = []
            settled = self.ledger.settled_tickets()
            # ticket_id -> reservation (query, source, page, account)
            ticket_index = {}
            for q in queries:
                for (source_name, page_id), rec in q["pages"].items():
                    if rec.get("ticket_id"):
                        ticket_index.setdefault(rec["ticket_id"], []).append(
                            {
                                "query_id": q["query_id"],
                                "account": q["account"],
                                "source": source_name,
                                "page_id": page_id,
                                "state": rec["state"],
                                "reserve_id": rec["reserve_id"],
                            }
                        )

            source_bytes = 0
            source_scans = 0
            settled_actual = 0
            pending_unknown_bytes = 0
            quarantined_bytes = 0
            per_source = {}
            for source_name, source in self.sources.items():
                entries = [
                    e
                    for e in source.audit_entries()
                    if query_id is None or e["query_id"] == str(query_id)
                ]
                per_source[source_name] = {
                    "scan_count": len(entries),
                    "scanned_bytes": sum(e["bytes"] for e in entries),
                    "bound_violations": sum(
                        1 for e in entries if e.get("bound_violation")
                    ),
                    "snapshot": source.snapshot,
                }
                source_bytes += per_source[source_name]["scanned_bytes"]
                source_scans += len(entries)
                for entry in entries:
                    tid = entry["ticket_id"]
                    owners = ticket_index.get(tid, [])
                    if not owners:
                        issues.append(
                            {
                                "kind": "unpaid_read",
                                "source": source_name,
                                "ticket_id": tid,
                                "bytes": entry["bytes"],
                                "detail": "source read a ticket with no query reservation",
                            }
                        )
                        continue
                    settlement = settled.get(tid)
                    owner = owners[0]
                    if settlement is not None:
                        if settlement["actual"] != entry["bytes"]:
                            issues.append(
                                {
                                    "kind": "bytes_mismatch",
                                    "source": source_name,
                                    "ticket_id": tid,
                                    "ledger": settlement["actual"],
                                    "source_audit": entry["bytes"],
                                }
                            )
                        settled_actual += settlement["actual"]
                    elif owner["state"] == "unknown":
                        # Real read, receipt lost: the obligation is held, not
                        # yet charged.  Not an error by itself.
                        pending_unknown_bytes += entry["bytes"]
                    elif owner["state"] == "quarantined":
                        # A real read whose cost exceeds its funded reservation;
                        # reported separately, never silently balanced.
                        quarantined_bytes += entry["bytes"]
                    else:
                        issues.append(
                            {
                                "kind": "read_without_settlement",
                                "source": source_name,
                                "ticket_id": tid,
                                "state": owner["state"],
                                "bytes": entry["bytes"],
                            }
                        )

            # Ledger-side spend for the selected queries (sum of settlements).
            ledger_spent = 0
            held_obligation = 0
            for q in queries:
                for rec in q["pages"].values():
                    if rec["state"] == "settled" and rec.get("settlement"):
                        ledger_spent += rec["settlement"]["actual"]
                    elif rec["state"] in {"unknown", "scanning", "active", "quarantined"}:
                        held_obligation += rec.get("reserved") or 0

            if ledger_spent != settled_actual:
                issues.append(
                    {
                        "kind": "total_mismatch",
                        "ledger_settled_actual": ledger_spent,
                        "source_audit_settled_bytes": settled_actual,
                    }
                )
            if pending_unknown_bytes:
                # Sources confirm reads whose receipts are still unknown; the
                # corresponding reservation must still cover them.
                if held_obligation < pending_unknown_bytes:
                    issues.append(
                        {
                            "kind": "underheld_unknown",
                            "pending_unknown_bytes": pending_unknown_bytes,
                            "held_obligation": held_obligation,
                        }
                    )
            if quarantined_bytes:
                # Isolation is reported; the books are not massaged to hide it.
                issues.append(
                    {
                        "kind": "source_bound_violation",
                        "source_audit_bytes": quarantined_bytes,
                        "detail": "read exceeded declared upper bound; quarantined, "
                        "not silently balanced",
                    }
                )

            account_checks = {}
            for name, state in self.ledger.accounts().items():
                if not state["invariant_ok"] or any(
                    state[k] < 0 for k in ("available", "reserved", "spent")
                ):
                    issues.append({"kind": "account_invariant", "account": name, **state})
                account_checks[name] = state

            return {
                "ok": not issues,
                "query_id": query_id,
                "issues": issues,
                "sources": per_source,
                "ledger": {
                    "settled_actual_for_queries": ledger_spent,
                    "held_obligation_for_unknown": held_obligation,
                    "pending_unknown_bytes": pending_unknown_bytes,
                    "quarantined_bytes": quarantined_bytes,
                    "source_scans": source_scans,
                    "source_bytes": source_bytes,
                },
                "accounts": account_checks,
                "queries": {
                    q["query_id"]: {
                        "status": q["status"],
                        "pages": {
                            f"{s}/{p}": rec["state"]
                            for (s, p), rec in sorted(q["pages"].items())
                        },
                    }
                    for q in queries
                },
            }

    # ----------------------------------------------------------------- helpers
    def _queries(self, query_id):
        if query_id not in self.queries:
            raise KeyError(f"unknown query {query_id!r}")
        return self.queries[query_id]

    def _page_bound(self, source, page_id):
        for page in source.catalog()["pages"]:
            if page["page_id"] == str(page_id):
                return int(page["upper_bound"])
        raise KeyError(f"unknown page {source.name}/{page_id}")

    def _new_page(self, source_name, page_id, reserve_id):
        return {
            "source": source_name,
            "page_id": str(page_id),
            "reserve_id": reserve_id,
            "ticket_id": None,
            "state": "reserved",
            "reserved": self.ledger.reservation(reserve_id)["amount"],
            "settlement": None,
            "release": None,
            "violation": None,
        }

    def _record_settlement(self, q, rec, call, ticket, settlement):
        rec["state"] = "settled"
        rec["ticket_id"] = ticket.get("ticket_id")
        rec["settlement"] = settlement
        rec["reserved"] = settlement.get("reserved", rec["reserved"])
        q["rows"][rec["source"]][rec["page_id"]] = list(ticket.get("rows", []))
        if call is not None:
            call["ticket_id"] = ticket.get("ticket_id")
            call["scanned_bytes"] = settlement["actual"]
            call["settlement"] = {
                "reserve_id": settlement["reserve_id"],
                "reserved": settlement["reserved"],
                "actual": settlement["actual"],
                "refund": settlement["refund"],
                "bound_violation": settlement.get("bound_violation", False),
            }
        call_status = "settled"
        if settlement.get("bound_violation"):
            call_status = "settled_bound_violation"
        if call is not None:
            call["status"] = call_status

    def _find_call(self, q, reserve_id):
        for call in q["scan_log"]:
            if call["reserve_id"] == reserve_id:
                return call
        return None

    def _safe_get_ticket(self, source, query_id, page_id):
        try:
            return source.get_ticket(query_id, page_id)
        except Exception:
            return None

    def _should_lose(self, spec, source_name, page_id):
        if spec is None or spec is False:
            return False
        if callable(spec):
            return bool(spec(source_name, str(page_id)))
        if spec is True:
            return True
        if isinstance(spec, (set, list, tuple)):
            return (source_name, str(page_id)) in {
                (s, str(p)) for s, p in spec
            }
        return False

    def _diagnostics(self, q):
        pages = []
        for (source_name, page_id), rec in sorted(q["pages"].items()):
            pages.append(
                {
                    "source": source_name,
                    "page_id": page_id,
                    "reserve_id": rec["reserve_id"],
                    "ticket_id": rec.get("ticket_id"),
                    "state": rec["state"],
                    "reserved": rec.get("reserved"),
                    "settlement": copy.deepcopy(rec.get("settlement")),
                    "violation": rec.get("violation"),
                }
            )
        scanned = [p for p in pages if p["state"] == "settled"]
        unknown = [p for p in pages if p["state"] == "unknown"]
        return {
            "pages": pages,
            "scan_log": copy.deepcopy(q["scan_log"]),
            "scanned_page_count": len(scanned),
            "unknown_page_count": len(unknown),
            "scanned_bytes": sum(
                (p["settlement"] or {}).get("actual", 0) for p in scanned
            ),
            "billed": sum(
                (p["settlement"] or {}).get("actual", 0) for p in scanned
            ),
            "refunded": sum(
                (p["settlement"] or {}).get("refund", 0) for p in scanned
            ),
            "scheduled_page_count": len(self._all_schedule(q)),
            "result_published": q["status"] == COMPLETED,
        }
