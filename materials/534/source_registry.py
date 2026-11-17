"""Source lifecycle, tickets and maintenance.

``SourceRegistry`` owns *only* source liveness and the right to read from a
source.  It never imports rows and never picks shards.

Interaction points (this is the whole contract):

* ``acquire(source, batch, shard)`` is the single choke point where a
  maintenance decision changes the next *real* source choice.  Once a request
  has taken effect no new ticket is handed out for that source.
* ``release(ticket)`` is the single choke point where finished/aborted work
  makes a real close possible.  If the source is DRAINING and this was the last
  physically live ticket, release drives the close callback.
* ``close_callback(source, generation)`` performs the real ``close`` on the
  underlying fixture.  It is *generation guarded*: a callback from a superseded
  generation (e.g. one queued before ``cancel_maintenance``) is refused, so an
  old callback can never close a source that has just been resumed.

Locking is ordered ``source.close_lock -> registry.lock``.  The real fixture
close happens while holding ``close_lock`` but *not* the registry lock, so a
cancel either waits for an in-progress close (and then refuses because the
source is genuinely closed) or wins the lock first and bumps the generation so
the pending close is rejected as stale.
"""

from __future__ import annotations

import hashlib
import threading
from typing import Dict, List, Optional

OPEN = "OPEN"
DRAINING = "DRAINING"
CLOSED = "CLOSED"
FAILED = "FAILED"

MAINT_ACTIVE = "ACTIVE"
MAINT_CANCELLED = "CANCELLED"
MAINT_CLOSED = "CLOSED"
MAINT_FAILED = "FAILED"


class RegistryError(RuntimeError):
    """Raised for a protocol violation (bad ticket, closed source, ...)."""

    def __init__(self, code, **detail):
        super().__init__(code)
        self.code = code
        self.detail = detail


class SourceRegistry:
    def __init__(self, sources: Dict[str, object], snapshots: Optional[Dict[str, str]] = None):
        self._lock = threading.RLock()
        self._sources = dict(sources)
        self._snap = dict(snapshots or {})
        self._state = {name: OPEN for name in self._sources}
        self._gen = {name: 0 for name in self._sources}  # bumps on every era change
        self._open_tickets: Dict[str, Dict[int, dict]] = {name: {} for name in self._sources}
        self._maint: Dict[str, Optional[dict]] = {name: None for name in self._sources}
        self._close_locks = {name: threading.RLock() for name in self._sources}
        self._maint_seq = 0
        self._ticket_seq = 0
        self._close_seq = 0

    # ------------------------------------------------------------------ reads
    def ticket_counts(self) -> Dict[str, int]:
        with self._lock:
            return {name: len(self._open_tickets[name]) for name in self._sources}

    def snapshot_of(self, source: str) -> Optional[str]:
        with self._lock:
            return self._source_snapshot(source)

    def _source_snapshot(self, source: str) -> Optional[str]:
        return self._snap.get(source, getattr(self._sources.get(source), "snapshot", None))

    def is_available(self, source: str) -> bool:
        with self._lock:
            if self._state.get(source) in (CLOSED, FAILED):
                return False
            maint = self._maint.get(source)
            if maint is not None and maint["status"] == MAINT_ACTIVE:
                return False
            return True

    # ----------------------------------------------------------- acquire/release
    def acquire(self, source: str, batch_id: str, shard_id: str) -> dict:
        """Hand out a read ticket, or explain why the source cannot be read.

        Check and registration are under one lock, so a request that has taken
        effect can never be overtaken by a late acquire.
        """
        with self._lock:
            if source not in self._sources:
                raise RegistryError("UNKNOWN_SOURCE", source=source)
            state = self._state[source]
            if state == CLOSED:
                raise RegistryError("SOURCE_CLOSED", source=source)
            if state == FAILED:
                raise RegistryError("SOURCE_FAILED", source=source)
            maint = self._maint[source]
            if maint is not None and maint["status"] == MAINT_ACTIVE:
                raise RegistryError("MAINTENANCE_ACTIVE", source=source, request_id=maint["request_id"])
            self._ticket_seq += 1
            ticket = {
                "ticket_id": self._ticket_seq,
                "source": source,
                "batch_id": batch_id,
                "shard_id": shard_id,
                "generation": self._gen[source],
                "state": "open",
                "aborted": None,
            }
            self._open_tickets[source][ticket["ticket_id"]] = ticket
            return ticket

    def mark_reading(self, ticket: dict) -> bool:
        """Atomically move a ticket open -> reading before the file is opened.

        Returns ``False`` when the ticket was already aborted by maintenance or
        superseded by a cancel, in which case the caller must not read.
        """
        with self._lock:
            source = ticket.get("source")
            live = self._open_tickets.get(source, {})
            if ticket.get("ticket_id") not in live:
                return False
            if ticket.get("generation") != self._gen[source]:
                return False
            if live[ticket["ticket_id"]]["state"] != "open":
                return False
            live[ticket["ticket_id"]]["state"] = "reading"
            return True

    def release(self, ticket: dict) -> bool:
        """Settle a ticket after its real read (or after it was never read).

        The physical ticket is always removed so it cannot leak.  Returns
        ``True`` when the ticket belonged to the live generation.  A stale
        ticket still releases the source reference but cannot influence the
        current era's close decision.
        """
        source = ticket.get("source")
        with self._lock:
            live = self._open_tickets.get(source, {})
            tid = ticket.get("ticket_id")
            if tid not in live:
                return False
            settled = ticket.get("generation") == self._gen[source]
            live[tid]["state"] = "released"
            del live[tid]
            maint = self._maint.get(source)
            if settled and maint is not None and maint["status"] == MAINT_ACTIVE:
                maint["completed_tickets"].append(tid)
            eligible = (
                maint is not None
                and maint["status"] == MAINT_ACTIVE
                and not live
            )
            gen = self._gen[source]
        if eligible:
            # A finished read is exactly what makes a close legal.  Drive it
            # here so maintenance does not depend on an external poller.
            self.close_callback(source, gen)
        return settled

    def abort_open_tickets(self, source: str, reason: str = "maintenance", batch_id: Optional[str] = None) -> List[dict]:
        """Abort tickets that have not yet started a real read.

        ``mark_reading`` is the instant a ticket starts reading, so anything
        still ``open`` has provably not touched the file and can safely be
        handed to a backup.  Aborted tickets are removed from the source's
        live set: they must not keep a drained source from closing.
        """
        with self._lock:
            aborted = []
            for ticket in list(self._open_tickets[source].values()):
                if batch_id is not None and ticket["batch_id"] != batch_id:
                    continue
                if ticket["state"] == "open":
                    ticket["state"] = "aborted"
                    ticket["aborted"] = reason
                    del self._open_tickets[source][ticket["ticket_id"]]
                    aborted.append(dict(ticket))
            return aborted

    def abort_ticket(self, ticket: dict, reason: str = "batch-cancelled") -> bool:
        with self._lock:
            source = ticket.get("source")
            tid = ticket.get("ticket_id")
            live = self._open_tickets.get(source, {})
            if tid not in live or live[tid]["state"] != "open":
                return False
            live[tid]["state"] = "aborted"
            live[tid]["aborted"] = reason
            del live[tid]
            return True

    def ticket_state(self, ticket: dict) -> Optional[str]:
        with self._lock:
            source = ticket.get("source")
            tid = ticket.get("ticket_id")
            if tid not in self._open_tickets.get(source, {}):
                return None
            return self._open_tickets[source][tid]["state"]

    # -------------------------------------------------------------- maintenance
    def request_maintenance(self, source: str, reason: str = "maintenance", request_id: Optional[str] = None) -> dict:
        """Start draining a source.  Accepted even while work is in flight.

        New acquisition is blocked immediately (this is what makes the next
        collection briefly pause / select a backup); tickets that are already
        reading keep working and can finish legally.
        """
        with self._lock:
            if source not in self._sources:
                raise RegistryError("UNKNOWN_SOURCE", source=source)
            state = self._state[source]
            current = self._maint[source]
            # Replaying the *same* request must return its progress and must
            # never reopen a source, even if it already finished.
            if current is not None and request_id is not None and current["request_id"] == request_id:
                return self._maint_view(source)
            if state == CLOSED:
                raise RegistryError("SOURCE_CLOSED", source=source)
            if state == FAILED:
                raise RegistryError("SOURCE_FAILED", source=source)
            if request_id is None:
                self._maint_seq += 1
                request_id = "maint-" + hashlib.sha1(
                    f"{source}:{reason}:{self._maint_seq}".encode()
                ).hexdigest()[:10]
            if current is not None and current["status"] == MAINT_ACTIVE:
                raise RegistryError("MAINTENANCE_BUSY", source=source, request_id=current["request_id"])
            # Take effect atomically.  New acquisition is blocked from this
            # instant, but every ticket already granted is *legal in-flight
            # work* and must be allowed to finish; only shards that have not
            # acquired a ticket get redirected to a qualified backup.
            self._gen[source] += 1
            self._maint[source] = {
                "request_id": request_id,
                "source": source,
                "reason": reason,
                "status": MAINT_ACTIVE,
                "generation": self._gen[source],
                "in_flight": [],
                "completed_tickets": [],
                "aborted_tickets": [],
                "close": None,
                "error": None,
            }
            # Adopt outstanding tickets (whether still ``open`` or already
            # ``reading``) into the maintenance era so their eventual release is
            # attributed to this request and keeps the source from closing early.
            # An acquired ticket is in flight even before bytes arrive; it is
            # never unconditionally re-dispatched.
            for ticket in self._open_tickets[source].values():
                ticket["generation"] = self._gen[source]
        # Self-driving: with no work at all the source may close right away.
        eligible, gen = self._refresh(source)
        if eligible:
            self.close_callback(source, gen)
        return self._maint_view(source)

    def tick(self, source: str) -> Optional[dict]:
        """Refresh observed in-flight work and possibly perform the real close."""
        eligible, gen = self._refresh(source)
        if eligible:
            self.close_callback(source, gen)
        return self._maint_view(source)

    def _refresh(self, source: str):
        with self._lock:
            maint = self._maint.get(source)
            if maint is None:
                return False, None
            if maint["status"] != MAINT_ACTIVE:
                return False, None
            live = sorted(self._open_tickets[source])
            maint["in_flight"] = live
            self._state[source] = DRAINING
            already_closed = bool(maint["close"]) and bool(maint["close"].get("ok"))
            eligible = not live and not already_closed
            return eligible, self._gen[source]

    def close_callback(self, source: str, generation: Optional[int] = None) -> dict:
        """Generation-guarded real close.

        Only the callback whose generation still matches the current era may
        close.  This is what prevents an old close callback from closing a
        source that a cancel just resumed.
        """
        with self._lock:
            self._close_seq += 1
            token = self._close_seq
            if generation is None:
                generation = self._gen[source]
        with self._close_locks[source]:
            with self._lock:
                if source not in self._sources:
                    return {"ok": False, "code": "UNKNOWN_SOURCE", "callback": token}
                maint = self._maint[source]
                if generation != self._gen[source]:
                    if maint is not None and maint["status"] == MAINT_ACTIVE:
                        # Do not mark success: the requesting era is stale.
                        maint["close"] = {
                            "ok": False,
                            "code": "STALE_CLOSE",
                            "callback": token,
                            "generation": generation,
                        }
                    return {"ok": False, "code": "STALE_CLOSE", "callback": token, "generation": generation}
                if maint is None or maint["status"] != MAINT_ACTIVE:
                    return {"ok": False, "code": "NO_ACTIVE_MAINTENANCE", "callback": token}
                if self._open_tickets[source]:
                    maint["in_flight"] = sorted(self._open_tickets[source])
                    return {
                        "ok": False,
                        "code": "IN_FLIGHT",
                        "callback": token,
                        "in_flight": maint["in_flight"],
                    }
                fixture = self._sources[source]
                if getattr(fixture, "closed", False):
                    maint["status"] = MAINT_CLOSED
                    self._state[source] = CLOSED
                    maint["close"] = {"ok": True, "callback": token, "external": True, "generation": generation}
                    return {"ok": True, "code": "ALREADY_CLOSED", "callback": token}
            # Real close outside the registry lock but under close_lock.  A
            # cancel must wait for close_lock, so it cannot interleave here.
            try:
                fixture.close()
            except RuntimeError as exc:
                code = str(exc)
                with self._lock:
                    maint = self._maint[source]
                    if maint is not None and maint["status"] == MAINT_ACTIVE:
                        maint["error"] = code
                        maint["close"] = {
                            "ok": False,
                            "code": code,
                            "callback": token,
                            "active": getattr(fixture, "active", None),
                            "in_flight": sorted(self._open_tickets[source]),
                            "generation": generation,
                        }
                        if code != "ACTIVE_READS":
                            maint["status"] = MAINT_FAILED
                            self._state[source] = FAILED
                        # ACTIVE_READS keeps the request ACTIVE and explainable.
                return {"ok": False, "code": code, "callback": token}
            with self._lock:
                if generation != self._gen[source]:
                    # Cannot happen while close_lock is held (cancel needs it),
                    # but kept as an explicit guard.
                    return {"ok": False, "code": "STALE_CLOSE", "callback": token}
                maint = self._maint[source]
                maint["close"] = {"ok": True, "callback": token, "generation": generation}
                maint["status"] = MAINT_CLOSED
                self._state[source] = CLOSED
                return {"ok": True, "code": "CLOSED", "callback": token}

    def cancel_maintenance(self, source: str, request_id: Optional[str] = None) -> dict:
        """Cancel a request iff the underlying source has not really closed.

        Acquires ``close_lock`` so it cannot race a real close: either it wins
        and bumps the generation (any pending close callback is then stale), or
        it waits for the close to finish and refuses because the source is
        genuinely closed.
        """
        with self._close_locks[source]:
            with self._lock:
                maint = self._maint.get(source)
                if maint is None or maint["status"] != MAINT_ACTIVE:
                    return {
                        "ok": False,
                        "code": "NO_ACTIVE_MAINTENANCE",
                        "maintenance": self._maint_view(source),
                    }
                if request_id is not None and maint["request_id"] != request_id:
                    return {
                        "ok": False,
                        "code": "REQUEST_MISMATCH",
                        "maintenance": self._maint_view(source),
                    }
                fixture = self._sources[source]
                if getattr(fixture, "closed", False):
                    maint["status"] = MAINT_CLOSED
                    self._state[source] = CLOSED
                    maint["close"] = {"ok": True, "external": True, "generation": self._gen[source]}
                    return {
                        "ok": False,
                        "code": "ALREADY_CLOSED",
                        "maintenance": self._maint_view(source),
                    }
                maint["status"] = MAINT_CANCELLED
                maint["close"] = {"ok": False, "code": "CANCELLED"}
                # A fresh era: old tickets stay stale, the source is usable.
                self._gen[source] += 1
                self._state[source] = OPEN
                return {"ok": True, "code": "CANCELLED", "maintenance": self._maint_view(source)}

    # ---------------------------------------------------------------- reporting
    def _maint_view(self, source: str) -> Optional[dict]:
        with self._lock:
            maint = self._maint.get(source)
            if maint is None:
                return None
            out = dict(maint)
            out["in_flight"] = sorted(self._open_tickets[source]) if maint["status"] == MAINT_ACTIVE else list(maint["in_flight"])
            out["completed_tickets"] = list(maint["completed_tickets"])
            out["aborted_tickets"] = list(maint["aborted_tickets"])
            out["close"] = None if maint["close"] is None else dict(maint["close"])
            return out

    def source_status(self, source: str) -> dict:
        with self._lock:
            if source not in self._sources:
                raise RegistryError("UNKNOWN_SOURCE", source=source)
            fixture = self._sources[source]
            return {
                "source": source,
                "state": self._state[source],
                "generation": self._gen[source],
                "snapshot": self._source_snapshot(source),
                "in_flight_tickets": sorted(self._open_tickets[source]),
                "in_flight": len(self._open_tickets[source]),
                "fixture_active": getattr(fixture, "active", None),
                "fixture_closed": getattr(fixture, "closed", None),
                "read_count": len(getattr(fixture, "reads", [])),
                "audit": list(getattr(fixture, "audit", [])),
                "maintenance": self._maint_view(source),
            }

    def status(self) -> dict:
        with self._lock:
            return {name: self.source_status(name) for name in self._sources}
