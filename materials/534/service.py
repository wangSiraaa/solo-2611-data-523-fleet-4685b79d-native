"""Orchestration service and CLI.

All process state lives here so the commands can be driven either in-process
(tests) or one process per command against the on-disk descriptor/status
files.  The two responsibilities (source registry and ingestion) are separate
objects that interact only through the documented choke points.
"""

from __future__ import annotations

import json
import os

from descriptor import build_descriptor, save_descriptor, load_descriptor, natural_key, shard_map
from ingestion import Ingestion
from source_registry import SourceRegistry


def make_sources(source_defs, snapshots=None):
    """Turn {name: ControlledSource} into fixtures with ``read_for`` adapters."""
    return dict(source_defs)


class Service:
    def __init__(self, workdir: str, source_defs, descriptor: dict):
        self.workdir = workdir
        os.makedirs(workdir, exist_ok=True)
        self.descriptor = descriptor
        self.registry = SourceRegistry(
            source_defs,
            snapshots={name: getattr(src, "snapshot", None) for name, src in source_defs.items()},
        )
        paths = {}
        for name, src in source_defs.items():
            mapped = {}
            for spec in descriptor["shards"]:
                mapped[spec["shard_id"]] = os.path.join(workdir, "sources", name, os.path.basename(spec["origin"]))
            paths[name] = mapped
        self.ingestion = Ingestion(
            self.registry,
            os.path.join(workdir, "ingest.db"),
            sources=source_defs,
            paths=paths,
            base_dir=self.workdir,
        )
        self.ingestion.register_batch(descriptor)

    # ------------------------------------------------------------------ ops
    def start_batch(self, deliver=True):
        batch = self.ingestion._batch(self.descriptor["batch_id"])
        results = []
        for sid in batch["order"]:
            outcome = self.ingestion._run(batch["batch_id"], sid, deliver=deliver, block=True)
            results.append(outcome)
        return {"batch_id": batch["batch_id"], "results": results}

    def step(self):
        return self.ingestion.step(self.descriptor["batch_id"])

    def maintain(self, request_id=None):
        return self.registry.request_maintenance(self.descriptor.get("primary", "A"), request_id=request_id)

    def cancel_maintenance(self, request_id=None):
        return self.registry.cancel_maintenance(self.descriptor.get("primary", "A"), request_id=request_id)

    def cancel_batch(self):
        return self.ingestion.cancel_batch(self.descriptor["batch_id"])

    def source_status(self):
        return self.registry.status()

    def batch_status(self):
        return self.ingestion.batch_status(self.descriptor["batch_id"])

    def verify(self):
        return self.ingestion.verify(self.descriptor["batch_id"])

    def write_descriptor(self):
        path = os.path.join(self.workdir, "descriptor.json")
        save_descriptor(self.descriptor, path)
        return path

    # ------------------------------------------------------------- persistence
    def export_state(self) -> dict:
        """Serialize the parts needed to continue across sequential CLI runs.

        Only the sequential subset is persisted: no process may exit with a
        real read in flight (that would be crash recovery, which is out of
        scope).  Tickets are in-process only.
        """
        batch = self.ingestion._batch(self.descriptor["batch_id"])
        with self.ingestion._batch_locks[batch["batch_id"]]:
            shards = {
                sid: {
                    "state": s["state"], "source": s["source"], "attempts": s["attempts"],
                    "rows": s["rows"], "last_error": s["last_error"],
                }
                for sid, s in batch["shards"].items()
            }
            cancel_epoch = batch["cancel_epoch"]
        sources = {}
        for name in self.registry._sources:
            st = self.registry.source_status(name)
            sources[name] = {
                "state": st["state"],
                "generation": st["generation"],
                "fixture_closed": st["fixture_closed"],
                "maintenance": st["maintenance"],
            }
        return {
            "batch_id": batch["batch_id"],
            "cancel_epoch": cancel_epoch,
            "shards": shards,
            "sources": sources,
        }

    def restore_state(self, state: dict) -> None:
        batch = self.ingestion._batch(self.descriptor["batch_id"])
        with self.ingestion._batch_locks[batch["batch_id"]]:
            batch["cancel_epoch"] = state.get("cancel_epoch", 0)
            for sid, saved in state.get("shards", {}).items():
                if sid not in batch["shards"]:
                    continue
                shard = batch["shards"][sid]
                shard["state"] = saved["state"]
                shard["source"] = saved.get("source")
                shard["attempts"] = saved.get("attempts", 0)
                shard["rows"] = saved.get("rows")
                shard["last_error"] = saved.get("last_error")
        for name, saved in state.get("sources", {}).items():
            if name not in self.registry._sources:
                continue
            fixture = self.registry._sources[name]
            if saved.get("fixture_closed") and hasattr(fixture, "force_closed"):
                fixture.force_closed()
            with self.registry._lock:
                self.registry._state[name] = saved["state"]
                self.registry._gen[name] = saved["generation"]
                self.registry._maint[name] = saved.get("maintenance")


def _json(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, indent=2)
