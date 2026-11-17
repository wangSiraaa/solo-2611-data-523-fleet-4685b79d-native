"""Independent, source-agnostic budget ledger.

The ledger is the only place that decides whether a page may be read and the
only place that moves credits.  It never trusts a caller-supplied "used" figure:
settlement is driven by a source receipt (the ticket), and the safe transitions
are enforced so a reservation can be charged *or* refunded at most once.

Invariant per account::

    initial == available + reserved + spent        (all values are ints >= 0)

``reserved`` is the sum of still-live reservations (``active``, ``scanning`` and
``quarantined``).  A ``quarantined`` reservation keeps its full amount held when
a source has already produced a receipt that costs more than the declared
upper bound: the ledger reports the violation instead of inventing extra
credits.
"""

from __future__ import annotations

import copy
import itertools
import threading


# --------------------------------------------------------------------- errors
class LedgerError(Exception):
    """Base class for ledger failures."""


class AccountNotFound(LedgerError):
    pass


class DuplicateAccount(LedgerError):
    pass


class InsufficientBudget(LedgerError):
    def __init__(self, account, needed, available):
        super().__init__(
            f"account {account!r} needs {needed} credits, only {available} available"
        )
        self.account = account
        self.needed = needed
        self.available = available


class ReservationNotFound(LedgerError):
    pass


class WrongAccount(LedgerError):
    pass


class TicketMismatch(LedgerError):
    """The receipt does not belong to this reservation (or was altered)."""


class ReservationConflict(LedgerError):
    """A terminal reservation cannot be moved to another terminal state."""


class RefundRequiresNotStarted(LedgerError):
    """Only an explicit source ``not_started`` receipt allows a refund."""


class SourceBoundViolation(LedgerError):
    """Actual bytes exceed the reserved/declared upper bound."""

    def __init__(self, reserve_id, actual, reserved, unfunded):
        super().__init__(
            f"reservation {reserve_id} scanned {actual} bytes but only "
            f"{reserved} were reserved ({unfunded} unfunded)"
        )
        self.reserve_id = reserve_id
        self.actual = actual
        self.reserved = reserved
        self.unfunded = unfunded


# ---------------------------------------------------------------------- ledger
class BudgetLedger:
    """Accounts, reservations, settlement, refunds and an audit trail.

    ``settle`` takes a *source ticket* dict and reads ``scanned_bytes`` from it.
    An optional ``verifier`` callable (e.g. ``PagedSource.verify_receipt``)
    returns the source's canonical receipt; the ledger then compares it with
    the caller's ticket, so a client cannot fabricate "expected bytes".
    """

    def __init__(self):
        self._accounts = {}
        self._reservations = {}
        self._settled_tickets = {}  # ticket_id -> settlement (cost is global)
        self._verifiers = {}  # source name -> canonical receipt verifier
        self._audit = []
        self._lock = threading.RLock()
        self._ids = itertools.count(1)

    def register_source(self, source_name, verifier):
        """Attach a source's receipt verifier.

        Once registered, every settlement against this source is canonicalized
        through ``verifier``; a client cannot supply its own "expected bytes".
        """
        with self._lock:
            self._verifiers[str(source_name)] = verifier

    # ---------------------------------------------------------------- accounts
    def open_account(self, account_id, credits):
        """Open an account with an integer, non-negative credit balance."""
        if not isinstance(credits, int) or isinstance(credits, bool) or credits < 0:
            raise ValueError("credits must be a non-negative int")
        with self._lock:
            if account_id in self._accounts:
                raise DuplicateAccount(f"account {account_id!r} already exists")
            self._accounts[account_id] = {
                "initial": credits,
                "available": credits,
                "reserved": 0,
                "spent": 0,
            }
            self._log({"event": "open_account", "account": account_id, "credits": credits})
            return self.state(account_id)

    def has_account(self, account_id):
        return account_id in self._accounts

    def _account(self, account_id):
        if account_id not in self._accounts:
            raise AccountNotFound(account_id)
        return self._accounts[account_id]

    # -------------------------------------------------------------- reservations
    def reserve(
        self,
        account_id,
        query_id,
        source,
        page_id,
        amount,
        *,
        ticket_id=None,
        reserve_id=None,
    ):
        """Reserve ``amount`` credits for one page identity.

        The check-and-decrement is atomic: when two connections race for the
        last credits, exactly the one that can actually pay gets a reservation.
        """
        return self.reserve_many(
            account_id,
            query_id,
            [
                {
                    "source": source,
                    "page_id": str(page_id),
                    "amount": amount,
                    "ticket_id": ticket_id,
                }
            ],
            reserve_ids=[reserve_id] if reserve_id is not None else None,
            reserve_prefix=reserve_id,
        )[0]

    def reserve_many(
        self,
        account_id,
        query_id,
        requests,
        *,
        reserve_ids=None,
        reserve_prefix=None,
    ):
        """Atomically reserve a conservative batch (used for whole-query plans).

        Either every request is reserved or none is.  This is the "reserve the
        whole query at once" strategy; per-page plans call ``reserve`` instead.
        """
        if not requests:
            return []
        if reserve_ids is not None and len(reserve_ids) != len(requests):
            raise ValueError("reserve_ids length mismatch")
        total = 0
        for req in requests:
            amount = req["amount"]
            if not isinstance(amount, int) or isinstance(amount, bool) or amount < 0:
                raise ValueError("reservation amount must be a non-negative int")
            total += amount

        with self._lock:
            account = self._account(account_id)
            if total > account["available"]:
                raise InsufficientBudget(account_id, total, account["available"])

            planned = []
            used_ids = set()
            for index, req in enumerate(requests):
                rid = reserve_ids[index] if reserve_ids else None
                if rid is None:
                    rid = f"{reserve_prefix or 'r'}-{next(self._ids)}"
                if rid in self._reservations or rid in used_ids:
                    raise DuplicateAccount(f"reserve_id {rid!r} already exists")
                used_ids.add(rid)
                if req.get("ticket_id") in self._settled_tickets:
                    raise TicketMismatch(
                        f"ticket {req.get('ticket_id')} was already settled; "
                        "do not reserve a page whose cost is already known"
                    )
                planned.append(
                    (
                        rid,
                        {
                            "reserve_id": rid,
                            "account": account_id,
                            "query_id": str(query_id),
                            "source": str(req["source"]),
                            "page_id": str(req["page_id"]),
                            "amount": int(req["amount"]),
                            "remaining": int(req["amount"]),
                            "ticket_id": req.get("ticket_id"),
                            "state": "active",
                        },
                    )
                )

            # commit
            for rid, reservation in planned:
                self._reservations[rid] = reservation
                account["available"] -= reservation["amount"]
                account["reserved"] += reservation["amount"]
                self._log(
                    {
                        "event": "reserve",
                        "account": account_id,
                        "query_id": str(query_id),
                        "reserve_id": rid,
                        "source": reservation["source"],
                        "page_id": reservation["page_id"],
                        "amount": reservation["amount"],
                        "ticket_id": reservation["ticket_id"],
                    }
                )
            return [rid for rid, _ in planned]

    def begin_scan(self, reserve_id, account_id=None):
        """Declare that a real scan is about to start.

        This closes the race with cancellation/refund: once a reservation is
        ``scanning``, it can no longer be refunded merely because a cancel
        arrived.  A refund then requires an explicit source ``not_started``
        receipt.
        """
        with self._lock:
            reservation = self._reservation(reserve_id, account_id)
            state = reservation["state"]
            if state == "scanning":
                return copy.deepcopy(reservation)  # idempotent
            if state != "active":
                raise ReservationConflict(
                    f"reservation {reserve_id} is {state}, cannot begin scan"
                )
            reservation["state"] = "scanning"
            self._log(
                {
                    "event": "begin_scan",
                    "reserve_id": reserve_id,
                    "account": reservation["account"],
                    "query_id": reservation["query_id"],
                    "source": reservation["source"],
                    "page_id": reservation["page_id"],
                }
            )
            return copy.deepcopy(reservation)

    # ------------------------------------------------------------------ settle
    def settle(self, reserve_id, ticket, *, account_id=None, verifier=None):
        """Transfer the ticket's actual bytes from the reservation to spent.

        ``ticket`` must be a source receipt with ``status == "done"`` and a
        ``scanned_bytes`` field.  The amount is taken from the receipt, never
        from a caller argument.  The unused part of the reservation is refunded.

        Duplicate settlement of the same ticket returns the original result and
        never charges twice.  A different amount under the same ticket id, a
        ticket for another page/query/source, or another account is rejected.
        """
        with self._lock:
            reservation = self._reservation(reserve_id, account_id)
            canonical = self._canonical_ticket(
                ticket, verifier or self._verifiers.get(reservation["source"])
            )
            self._check_ticket_identity(reserve_id, reservation, canonical)
            actual = canonical.get("scanned_bytes")
            if not isinstance(actual, int) or isinstance(actual, bool) or actual < 0:
                raise TicketMismatch("ticket scanned_bytes must be a non-negative int")
            tid = canonical.get("ticket_id")

            # ---- already settled? idempotent or a conflict.
            if reservation["state"] in {"settled", "duplicate_refund"}:
                record = reservation.get("settlement")
                if tid == record["ticket_id"] and actual == record["actual"]:
                    return copy.deepcopy(record)
                raise TicketMismatch(
                    f"reservation {reserve_id} was settled for ticket "
                    f"{record['ticket_id']} / {record['actual']} bytes"
                )
            if reservation["state"] == "quarantined":
                raise ReservationConflict(
                    f"reservation {reserve_id} is quarantined: {reservation.get('violation')}"
                )
            if reservation["state"] == "released":
                raise ReservationConflict(
                    f"reservation {reserve_id} was refunded; a real scan cannot be charged to it"
                )

            # ---- the same source ticket may only be paid once, globally.
            prior = self._settled_tickets.get(tid)
            if prior is not None:
                if prior["actual"] != actual:
                    raise TicketMismatch(
                        f"ticket {tid} was settled for {prior['actual']} bytes, "
                        f"not {actual}"
                    )
                if prior["account"] != reservation["account"]:
                    raise WrongAccount(
                        f"ticket {tid} already belongs to account {prior['account']!r}"
                    )
                # No new read happened, therefore no new cost: refund this
                # reservation fully and return the original settlement.
                account = self._account(reservation["account"])
                amount = reservation["amount"]
                account["reserved"] -= amount
                account["available"] += amount
                reservation["state"] = "duplicate_refund"
                reservation["settlement"] = prior
                self._check_invariant(reservation["account"])
                self._log(
                    {
                        "event": "settle_duplicate",
                        "reserve_id": reserve_id,
                        "ticket_id": tid,
                        "actual": actual,
                        "refund": amount,
                        "original_reserve_id": prior["reserve_id"],
                    }
                )
                return copy.deepcopy(prior)

            # ---- normal settlement from the receipt.
            amount = reservation["amount"]
            account = self._account(reservation["account"])
            bound_violation = actual > amount
            result = {
                "reserve_id": reserve_id,
                "account": reservation["account"],
                "query_id": reservation["query_id"],
                "source": reservation["source"],
                "page_id": reservation["page_id"],
                "ticket_id": tid,
                "reserved": amount,
                "actual": actual,
                "refund": max(0, amount - actual),
                "bound_violation": bound_violation,
                "state": "settled",
            }

            if not bound_violation:
                account["reserved"] -= amount
                account["available"] += amount - actual
                account["spent"] += actual
                reservation["state"] = "settled"
                reservation["settlement"] = result
            else:
                # Source violated the catalog guarantee: actual bytes exceed
                # the declared/reserved upper bound.  Isolate and report; do
                # not quietly top up from available credits (that would hide
                # the violation and steal funds from other queries), and do
                # not fabricate a balanced book.  The reservation stays held
                # and the unfunded shortfall is reported.
                shortfall = actual - amount
                reservation["state"] = "quarantined"
                reservation["violation"] = {
                    "ticket_id": tid,
                    "actual": actual,
                    "reserved": amount,
                    "shortfall": shortfall,
                    "unfunded": shortfall,
                    "source": reservation["source"],
                    "page_id": reservation["page_id"],
                }
                self._log(
                    {
                        "event": "bound_violation",
                        "reserve_id": reserve_id,
                        "ticket_id": tid,
                        "actual": actual,
                        "reserved": amount,
                        "unfunded": shortfall,
                        "quarantined": True,
                    }
                )
                self._check_invariant(reservation["account"])
                raise SourceBoundViolation(reserve_id, actual, amount, shortfall)

            self._settled_tickets[tid] = dict(result)
            self._check_invariant(reservation["account"])
            self._log(
                {
                    "event": "settle",
                    "reserve_id": reserve_id,
                    "account": reservation["account"],
                    "query_id": reservation["query_id"],
                    "source": reservation["source"],
                    "page_id": reservation["page_id"],
                    "ticket_id": tid,
                    "reserved": amount,
                    "actual": actual,
                    "refund": result["refund"],
                    "bound_violation": bound_violation,
                }
            )
            return copy.deepcopy(result)

    def _canonical_ticket(self, ticket, verifier):
        if ticket is None:
            raise TicketMismatch("missing source ticket")
        if verifier is not None:
            return verifier(ticket)
        if ticket.get("status") != "done":
            raise TicketMismatch(
                f"ticket status {ticket.get('status')!r} is not a completed scan"
            )
        return copy.deepcopy(ticket)

    def _check_ticket_identity(self, reserve_id, reservation, ticket):
        if ticket.get("status") != "done":
            raise TicketMismatch("only a completed receipt can be settled")
        if str(ticket.get("source")) != reservation["source"]:
            raise TicketMismatch(
                f"ticket source {ticket.get('source')!r} != reservation source "
                f"{reservation['source']!r}"
            )
        if str(ticket.get("page_id")) != reservation["page_id"]:
            raise TicketMismatch(
                f"ticket page {ticket.get('page_id')!r} != reservation page "
                f"{reservation['page_id']!r}"
            )
        if str(ticket.get("query_id")) != reservation["query_id"]:
            raise TicketMismatch(
                f"ticket query {ticket.get('query_id')!r} != reservation query "
                f"{reservation['query_id']!r}"
            )
        if reservation.get("ticket_id") is not None and ticket.get(
            "ticket_id"
        ) != reservation["ticket_id"]:
            raise TicketMismatch(
                f"ticket id {ticket.get('ticket_id')!r} != reservation ticket "
                f"{reservation['ticket_id']!r}"
            )

    # ------------------------------------------------------------------ refund
    def release(self, reserve_id, evidence=None, *, account_id=None, reason=None):
        """Refund a reservation only with explicit source evidence ``not_started``.

        A scan that has happened (``done``) must be settled.  An ``unknown``
        ticket keeps its full obligation.  Cancellation, an empty result or a
        connection error is *not* evidence of a free page.
        """
        with self._lock:
            reservation = self._reservation(reserve_id, account_id)
            state = reservation["state"]
            if state == "released":
                return copy.deepcopy(reservation["release"])
            if state in {"settled", "duplicate_refund", "quarantined"}:
                raise ReservationConflict(
                    f"reservation {reserve_id} is {state}, it cannot be refunded"
                )

            if not evidence or evidence.get("status") != "not_started":
                raise RefundRequiresNotStarted(
                    f"reservation {reserve_id}: refund requires a source "
                    "'not_started' receipt, got "
                    f"{None if evidence is None else evidence.get('status')!r}"
                )
            if (
                reservation.get("ticket_id") is not None
                and evidence.get("ticket_id") != reservation["ticket_id"]
            ):
                raise TicketMismatch(
                    "not_started evidence is for a different ticket"
                )

            account = self._account(reservation["account"])
            amount = reservation["amount"]
            account["reserved"] -= amount
            account["available"] += amount
            result = {
                "reserve_id": reserve_id,
                "account": reservation["account"],
                "query_id": reservation["query_id"],
                "source": reservation["source"],
                "page_id": reservation["page_id"],
                "ticket_id": reservation.get("ticket_id"),
                "refund": amount,
                "state": "released",
                "reason": reason,
            }
            reservation["state"] = "released"
            reservation["release"] = result
            self._check_invariant(reservation["account"])
            self._log(
                {
                    "event": "release",
                    "reserve_id": reserve_id,
                    "account": reservation["account"],
                    "query_id": reservation["query_id"],
                    "source": reservation["source"],
                    "page_id": reservation["page_id"],
                    "refund": amount,
                    "reason": reason,
                }
            )
            return copy.deepcopy(result)

    # --------------------------------------------------------------- inspection
    def _reservation(self, reserve_id, account_id=None):
        if reserve_id not in self._reservations:
            raise ReservationNotFound(reserve_id)
        reservation = self._reservations[reserve_id]
        if account_id is not None and reservation["account"] != account_id:
            raise WrongAccount(
                f"reservation {reserve_id} belongs to {reservation['account']!r}, "
                f"not {account_id!r}"
            )
        return reservation

    def reservation(self, reserve_id, account_id=None):
        with self._lock:
            return copy.deepcopy(self._reservation(reserve_id, account_id))

    def reservations_for(self, *, query_id=None, account_id=None):
        with self._lock:
            return [
                copy.deepcopy(r)
                for r in self._reservations.values()
                if (query_id is None or r["query_id"] == query_id)
                and (account_id is None or r["account"] == account_id)
            ]

    def is_live(self, reserve_id):
        with self._lock:
            r = self._reservations.get(reserve_id)
            return r is not None and r["state"] in {"active", "scanning"}

    def state(self, account_id):
        with self._lock:
            account = self._account(account_id)
            return {
                "account": account_id,
                "initial": account["initial"],
                "available": account["available"],
                "reserved": account["reserved"],
                "spent": account["spent"],
                "balance": account["available"] + account["reserved"] + account["spent"],
                "invariant_ok": account["available"] + account["reserved"] + account["spent"]
                == account["initial"],
            }

    def accounts(self):
        with self._lock:
            return {name: self.state(name) for name in sorted(self._accounts)}

    def audit(self, *, account_id=None, query_id=None):
        with self._lock:
            return copy.deepcopy(
                [
                    entry
                    for entry in self._audit
                    if (account_id is None or entry.get("account") == account_id)
                    and (query_id is None or entry.get("query_id") == query_id)
                ]
            )

    def settled_tickets(self):
        with self._lock:
            return copy.deepcopy(self._settled_tickets)

    def totals(self):
        with self._lock:
            return {
                "available": sum(a["available"] for a in self._accounts.values()),
                "reserved": sum(a["reserved"] for a in self._accounts.values()),
                "spent": sum(a["spent"] for a in self._accounts.values()),
                "initial": sum(a["initial"] for a in self._accounts.values()),
            }

    # ------------------------------------------------------------------ helpers
    def _check_invariant(self, account_id):
        state = self.state(account_id)
        for key in ("available", "reserved", "spent"):
            if state[key] < 0:
                raise LedgerError(f"account {account_id} has negative {key}")
        if not state["invariant_ok"]:
            raise LedgerError(
                f"account {account_id} invariant broken: {state}"
            )

    def _log(self, entry):
        self._audit.append(entry)


# Backwards-friendly alias used by some callers.
CostAccount = BudgetLedger
