"""Bounded JSONL page source.

The source is deliberately dumb: it knows how to expose a catalog and how to
read one page.  It does not reserve or settle budget.  A ticket is a stable
function of ``(source name, snapshot, query_id, page_id)``; asking for the same
ticket again returns the already-read page and never reads (or audits) the file
a second time.

Cost is the number of bytes actually read from the file.  The bytes include raw
newlines because pages are built from ``splitlines(keepends=True)``.  The
catalog's ``upper_bound`` is a conservative, declared bound: by default it is
the exact page size, but tests may add padding (or deliberately under-state it)
to exercise refunds and source-bound violations.
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
from pathlib import Path


class SourceError(Exception):
    """Base class for source failures."""


class UnknownPage(SourceError):
    """The requested page id is not in the catalog."""


class SourceChanged(SourceError):
    """The file no longer matches the snapshot recorded in the catalog."""


class PagedSource:
    """A local, immutable, paged JSONL source with ticket-based receipts."""

    def __init__(
        self,
        name: str,
        path,
        page_rows: int = 1,
        *,
        bound_padding: int = 0,
        bound_override=None,
    ):
        if page_rows < 1:
            raise ValueError("page_rows must be >= 1")
        if bound_padding < 0:
            raise ValueError("bound_padding must be >= 0")

        self.name = str(name)
        self.path = Path(path)
        raw = self.path.read_bytes()
        self.snapshot = hashlib.sha256(raw).hexdigest()

        lines = raw.splitlines(keepends=True)
        self.pages = []
        offset = 0
        for index, start in enumerate(range(0, len(lines), page_rows)):
            chunk = b"".join(lines[start : start + page_rows])
            actual_bytes = len(chunk)
            if bound_override is not None:
                upper_bound = int(bound_override(index, actual_bytes))
            else:
                upper_bound = actual_bytes + bound_padding
            self.pages.append(
                {
                    "page_id": str(index),
                    "offset": offset,
                    # Public, conservative charge bound (includes newlines).
                    "upper_bound": upper_bound,
                    "sha256": hashlib.sha256(chunk).hexdigest(),
                    # Private; not exposed by catalog().
                    "_actual_bytes": actual_bytes,
                }
            )
            offset += actual_bytes

        if offset != len(raw):  # defensive invariant
            raise SourceError("pagination did not consume the whole file")

        self.receipts = {}
        self.pending_resolution = set()
        self.read_counts = {}
        self.audit = []
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ catalog
    def catalog(self):
        """Public metadata: snapshot, page count, and per-page upper bounds."""
        with self._lock:
            return {
                "source": self.name,
                "snapshot": self.snapshot,
                "page_count": len(self.pages),
                "pages": [
                    {
                        "page_id": p["page_id"],
                        "offset": p["offset"],
                        "upper_bound": p["upper_bound"],
                        "sha256": p["sha256"],
                    }
                    for p in self.pages
                ],
            }

    def upper_bound(self):
        """Total declared upper bound for every page in this source."""
        with self._lock:
            return sum(p["upper_bound"] for p in self.pages)

    def page_ids(self):
        with self._lock:
            return [p["page_id"] for p in self.pages]

    def ticket_id(self, query_id, page_id):
        payload = json.dumps(
            [self.name, self.snapshot, str(query_id), str(page_id)],
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # --------------------------------------------------------------------- scan
    def _find_page(self, page_id):
        wanted = str(page_id)
        page = next((p for p in self.pages if p["page_id"] == wanted), None)
        if page is None:
            raise UnknownPage(f"unknown page {self.name}/{wanted}")
        return page

    def scan(self, query_id, page_id, lose_response: bool = False):
        """Read a page once and return a ticket/receipt.

        The actual file read and the source-side audit entry happen at most once
        per ticket.  ``lose_response=True`` simulates the read succeeding while
        the response is lost: the receipt exists, the caller sees TimeoutError,
        and ``get_ticket`` reports ``unknown`` until the test resolves it.
        """
        with self._lock:
            page = self._find_page(page_id)
            page_id = page["page_id"]
            tid = self.ticket_id(query_id, page_id)

            if tid not in self.receipts:
                with self.path.open("rb") as fh:
                    fh.seek(page["offset"])
                    raw = fh.read(page["_actual_bytes"])
                if len(raw) != page["_actual_bytes"]:
                    raise SourceChanged(
                        f"short read for {self.name}/{page_id}: "
                        f"{len(raw)} != {page['_actual_bytes']}"
                    )
                if hashlib.sha256(raw).hexdigest() != page["sha256"]:
                    raise SourceChanged(f"snapshot changed for {self.name}/{page_id}")

                rows = [json.loads(line) for line in raw.splitlines()]
                receipt = {
                    "status": "done",
                    "ticket_id": tid,
                    "query_id": str(query_id),
                    "source": self.name,
                    "snapshot": self.snapshot,
                    "page_id": page_id,
                    "scanned_bytes": len(raw),
                    "upper_bound": page["upper_bound"],
                    "actual_exceeds_bound": len(raw) > page["upper_bound"],
                    "rows": rows,
                }
                self.receipts[tid] = receipt
                self.read_counts[tid] = self.read_counts.get(tid, 0) + 1
                self.audit.append(
                    {
                        "op": "scan",
                        "query_id": str(query_id),
                        "source": self.name,
                        "snapshot": self.snapshot,
                        "page_id": page_id,
                        "ticket_id": tid,
                        "bytes": len(raw),
                        "upper_bound": page["upper_bound"],
                        "bound_violation": len(raw) > page["upper_bound"],
                    }
                )

            if lose_response:
                self.pending_resolution.add(tid)
                raise TimeoutError(
                    f"response lost after scanning {self.name}/{page_id}"
                )

            # A successful retry delivers the receipt, so it also resolves a
            # previously-lost response for this ticket.
            self.pending_resolution.discard(tid)
            return copy.deepcopy(self.receipts[tid])

    def get_ticket(self, query_id, page_id):
        """Return ``done``, ``not_started``, or ``unknown`` for a page ticket.

        ``not_started`` is an explicit statement that no read happened.  It is
        the only source-side evidence that permits a reservation refund.
        """
        with self._lock:
            page = self._find_page(page_id)
            page_id = page["page_id"]
            tid = self.ticket_id(query_id, page_id)
            if tid in self.pending_resolution:
                return {
                    "status": "unknown",
                    "ticket_id": tid,
                    "query_id": str(query_id),
                    "source": self.name,
                    "snapshot": self.snapshot,
                    "page_id": page_id,
                }
            if tid in self.receipts:
                return copy.deepcopy(self.receipts[tid])
            return {
                "status": "not_started",
                "ticket_id": tid,
                "query_id": str(query_id),
                "source": self.name,
                "snapshot": self.snapshot,
                "page_id": page_id,
            }

    def verify_receipt(self, ticket):
        """Canonicalize a ticket against the source's own receipt store.

        The ledger uses this so a client cannot submit fabricated bytes.  It
        accepts the ticket's ``ticket_id`` and returns the source's copy; any
        mismatch (including a changed ``scanned_bytes``) raises ``SourceError``.
        """
        with self._lock:
            if not isinstance(ticket, dict) or "ticket_id" not in ticket:
                raise SourceError("receipt has no ticket_id")
            tid = ticket["ticket_id"]
            if tid in self.pending_resolution:
                raise SourceError(f"ticket {tid} is unresolved (unknown)")
            receipt = self.receipts.get(tid)
            if receipt is None:
                raise SourceError(f"source has no receipt for ticket {tid}")
            # If the caller included fields, they must agree with the source.
            for field in ("status", "source", "page_id", "query_id", "scanned_bytes"):
                if field in ticket and ticket[field] != receipt[field]:
                    raise SourceError(
                        f"ticket {tid} field {field!r} does not match source receipt"
                    )
            return copy.deepcopy(receipt)

    def resolve_for_test(self, query_id, page_id):
        """Test hook: make a lost response visible again."""
        with self._lock:
            page = self._find_page(page_id)
            tid = self.ticket_id(query_id, page["page_id"])
            self.pending_resolution.discard(tid)
            return self.get_ticket(query_id, page["page_id"])

    # -------------------------------------------------------------------- audit
    def audit_totals(self):
        with self._lock:
            return {
                "scan_count": len(self.audit),
                "scanned_bytes": sum(e["bytes"] for e in self.audit),
                "tickets": sorted({e["ticket_id"] for e in self.audit}),
            }

    def audit_entries(self):
        with self._lock:
            return copy.deepcopy(self.audit)

    def actual_reads(self):
        """Number of real file reads, independent of caller-side counters."""
        with self._lock:
            return sum(self.read_counts.values())
