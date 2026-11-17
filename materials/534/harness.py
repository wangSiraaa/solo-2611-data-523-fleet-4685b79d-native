"""Scenario harness: real A/B source files, composite fixtures and workspaces."""

from __future__ import annotations

import hashlib
import os
import shutil
import threading

from descriptor import build_descriptor, natural_key, save_descriptor, validate_descriptor
from controlled_source import ControlledSource


class CompositeSource:
    """A logical source holding one real file per shard.

    ``close`` refuses if *any* underlying read is still active, mirroring the
    single-file fixture.  Pause gates are per (source, shard) so a scenario can
    hold one shard while letting others run.
    """

    def __init__(self, name, file_map, snapshot="frozen-1", close_hook=None):
        self.name = name
        self.snapshot = snapshot
        self.files = {}
        for shard_id, path in file_map.items():
            self.files[shard_id] = ControlledSource(name, path, snapshot=snapshot, close_hook=close_hook)
        self.close_hook = close_hook
        self.lock = threading.RLock()

    # -- status ------------------------------------------------------------
    @property
    def active(self):
        return sum(f.active for f in self.files.values())

    @property
    def closed(self):
        return all(f.closed for f in self.files.values()) if self.files else False

    @property
    def audit(self):
        out = []
        for shard_id, f in self.files.items():
            for entry in f.audit:
                out.append(dict(entry, shard=shard_id))
        return out

    @property
    def reads(self):
        return [n for f in self.files.values() for n in f.reads]

    def fixture(self, shard_id):
        return self.files[shard_id]

    # -- read adapter ------------------------------------------------------
    def read_for(self, source, shard_id, path):
        return self.files[shard_id].read()

    def read(self, source=None, shard_id=None, path=None):
        if shard_id is None:
            # Single-file fallback: first shard (used only by tiny probes).
            shard_id = next(iter(self.files))
        return self.files[shard_id].read()

    # -- close -------------------------------------------------------------
    def close(self):
        with self.lock:
            active = self.active
            if active:
                raise RuntimeError("ACTIVE_READS")
            if self.close_hook is not None:
                self.close_hook(self)
            for shard_id, f in self.files.items():
                if not f.closed:
                    f.close()

    def force_closed(self):
        """Reflect a persisted CLOSED state when a fresh process rebuilds us."""
        with self.lock:
            for f in self.files.values():
                f.closed = True


def _write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write("row_id,value\n")
        for row_id, value in rows:
            fh.write(f"{row_id},{value}\n")
    with open(path, "rb") as fh:
        raw = fh.read()
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}


def build_workspace(
    root,
    batch_id="batch-534",
    snapshot="frozen-1",
    num_shards=2,
    rows_per_shard=50,
    b_overrides=None,
    b_snapshot=None,
):
    """Create real source trees, the descriptor, and the fixture objects.

    ``b_overrides`` maps shard_id -> list of (row_id, value).  A shard listed
    there is a *divergent* backup file name that must be rejected by digest.
    ``b_snapshot`` may set a different snapshot for B (defaults to same).
    """
    if os.path.exists(root):
        shutil.rmtree(root)
    src_a = os.path.join(root, "sources", "A")
    src_b = os.path.join(root, "sources", "B")
    shard_files = {}
    a_map = {}
    b_map = {}
    b_overrides = b_overrides or {}
    for idx in range(1, num_shards + 1):
        shard_id = f"shard-{idx}"
        rel = f"{shard_id}.csv"
        rows = [(f"r{idx}-{n}", (idx * 100) + n) for n in range(1, rows_per_shard + 1)]
        path_a = os.path.join(src_a, rel)
        _write_csv(path_a, rows)
        shard_files[shard_id] = path_a
        a_map[shard_id] = path_a
        b_rows = b_overrides.get(shard_id, rows)
        path_b = os.path.join(src_b, rel)
        _write_csv(path_b, b_rows)
        b_map[shard_id] = path_b
    desc = build_descriptor(
        batch_id,
        snapshot,
        shard_files,
        origin_dir=root,
        primary="A",
        backups=["B"],
    )
    validate_descriptor(desc)
    save_descriptor(desc, os.path.join(root, "descriptor.json"))
    a = CompositeSource("A", a_map, snapshot=snapshot)
    b = CompositeSource("B", b_map, snapshot=b_snapshot or snapshot)
    return {"descriptor": desc, "A": a, "B": b, "a_map": a_map, "b_map": b_map,
            "a_dir": src_a, "b_dir": src_b}


def tamper_bytes(path):
    with open(path, "ab") as fh:
        fh.write(b"extra,1\n")
