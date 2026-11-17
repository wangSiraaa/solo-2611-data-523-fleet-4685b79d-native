"""Controllable local source fixture.

The fixture is intentionally read-only: ``read`` only ever opens the file for
reading and ``close`` refuses while a real read is still active.  Tests can
pause a read before the file is opened (``allow_read``) and before the bytes
are returned to the caller (``allow_callback``).
"""

from __future__ import annotations

import hashlib
import os
import threading


class ControlledSource:
    def __init__(self, name, path, snapshot="frozen-1", close_hook=None):
        self.name = name
        self.path = path
        self.snapshot = snapshot
        self.active = 0
        self.closed = False
        self.lock = threading.Lock()
        self.before_read = threading.Event()
        self.allow_read = threading.Event()
        self.allow_read.set()
        self.before_callback = threading.Event()
        self.allow_callback = threading.Event()
        self.allow_callback.set()
        self.audit = []
        # Optional hook invoked while the fixture lock is held, just before the
        # real close.  Used to exercise close/cancel interleavings.
        self.close_hook = close_hook
        self.reads = []

    # -- descriptor helpers -------------------------------------------------
    def raw(self):
        with open(self.path, "rb") as fh:
            return fh.read()

    def descriptor(self):
        raw = self.raw()
        return {
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "snapshot": self.snapshot,
        }

    # -- real I/O -----------------------------------------------------------
    def read(self):
        with self.lock:
            if self.closed:
                raise RuntimeError("SOURCE_CLOSED")
            self.active += 1
            self.audit.append({"op": "read-start", "active": self.active, "path": os.path.basename(self.path)})
        try:
            self.before_read.set()
            if not self.allow_read.wait(10):
                raise RuntimeError("READ_GATE_TIMEOUT")
            with open(self.path, "rb") as fh:
                raw = fh.read()
            self.before_callback.set()
            if not self.allow_callback.wait(10):
                raise RuntimeError("CALLBACK_GATE_TIMEOUT")
            self.reads.append(len(raw))
            return raw
        finally:
            with self.lock:
                self.active -= 1
                self.audit.append({"op": "read-end", "active": self.active, "path": os.path.basename(self.path)})

    def close(self):
        with self.lock:
            if self.closed:
                raise RuntimeError("ALREADY_CLOSED")
            if self.active:
                self.audit.append({"op": "close-refused", "active": self.active})
                raise RuntimeError("ACTIVE_READS")
            if self.close_hook is not None:
                self.close_hook(self)
            self.closed = True
            self.audit.append({"op": "close", "active": 0})
