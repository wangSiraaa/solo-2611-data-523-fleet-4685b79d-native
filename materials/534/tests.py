#!/usr/bin/env python3.11
"""Acceptance tests for the batch collection service.

Run: python3.11 tests.py
"""

import json
import os
import tempfile
import threading
import time
import unittest

from descriptor import (
    build_descriptor,
    descriptor_sha256,
    load_descriptor,
    parse_csv_bytes,
    validate_descriptor,
    verify_shard_file,
)
from harness import build_workspace, CompositeSource, _write_csv
from service import Service


def make(root, **kwargs):
    ws = build_workspace(root, **kwargs)
    svc = Service(root, {"A": ws["A"], "B": ws["B"]}, ws["descriptor"])
    return ws, svc


class CoreMaintenanceDrain(unittest.TestCase):
    def test_shard1_a_drains_shard2_b_then_close(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=2, rows_per_shard=50)
        A, B = ws["A"], ws["B"]
        f1 = A.fixture("shard-1")
        f1.allow_read.clear()
        t1 = svc.ingestion.dispatch("batch-534", "shard-1")
        self.assertTrue(f1.before_read.wait(3))

        m = svc.maintain("m1")
        self.assertEqual(m["status"], "ACTIVE")
        self.assertIn(1, m["in_flight"])
        self.assertEqual(svc.source_status()["A"]["state"], "DRAINING")

        # shard-2 must really read B now
        out = svc.ingestion._run("batch-534", "shard-2", deliver=True, block=True)
        self.assertEqual(out["status"], "COMPLETE")
        self.assertEqual(out["source"], "B")
        self.assertIn("read-start", [e["op"] for e in B.audit])

        # A still cannot close while shard-1 is really reading
        self.assertFalse(A.closed)
        self.assertEqual(svc.source_status()["A"]["state"], "DRAINING")

        f1.allow_read.set()
        t1.join(5)
        self.assertFalse(t1.is_alive())

        st = svc.batch_status()
        self.assertEqual(st["state"], "COMPLETE")
        self.assertEqual(st["rows"], 100)
        self.assertEqual(svc.source_status()["A"]["state"], "CLOSED")
        self.assertTrue(A.closed)

        # shard-1 imported exactly once, from A; shard-2 from B
        done = dict(svc.ingestion.db.execute("SELECT shard_id, source FROM shards_done").fetchall())
        self.assertEqual(done, {"shard-1": "A", "shard-2": "B"})
        v = svc.verify()
        self.assertTrue(v["ok"], v)
        self.assertEqual(v["origin_total"], v["db_total"])


class DuplicateMaintenance(unittest.TestCase):
    def test_replay_returns_progress_and_does_not_reopen(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=1)
        f = ws["A"].fixture("shard-1")
        f.allow_read.clear()
        t = svc.ingestion.dispatch("batch-534", "shard-1")
        self.assertTrue(f.before_read.wait(3))

        m1 = svc.maintain("req-1")
        m2 = svc.maintain("req-1")
        self.assertEqual(m1["request_id"], m2["request_id"])
        self.assertEqual(m2["status"], "ACTIVE")
        self.assertEqual(m1["in_flight"], m2["in_flight"])

        with self.assertRaises(Exception) as ctx:
            svc.maintain("req-2")
        self.assertEqual(getattr(ctx.exception, "code", None), "MAINTENANCE_BUSY")

        f.allow_read.set()
        t.join(5)
        self.assertEqual(svc.source_status()["A"]["state"], "CLOSED")
        self.assertTrue(ws["A"].closed)

        # replaying a finished request returns its receipt, never reopens
        m3 = svc.registry.request_maintenance("A", "req-replay", request_id="req-1")
        self.assertEqual(m3["status"], "CLOSED")
        self.assertTrue(ws["A"].closed)


class CancelMaintenanceRace(unittest.TestCase):
    def test_cancel_before_close_restores_and_stale_callback_refused(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=1)
        f = ws["A"].fixture("shard-1")
        f.allow_read.clear()
        t = svc.ingestion.dispatch("batch-534", "shard-1")
        self.assertTrue(f.before_read.wait(3))

        m = svc.registry.request_maintenance("A", reason="m")
        gen = m["generation"]
        res = svc.registry.cancel_maintenance("A", m["request_id"])
        self.assertTrue(res["ok"])
        self.assertEqual(res["code"], "CANCELLED")
        self.assertEqual(svc.source_status()["A"]["state"], "OPEN")

        f.allow_read.set()
        t.join(5)

        # an old close callback from the cancelled era must not close A
        stale = svc.registry.close_callback("A", gen)
        self.assertFalse(stale["ok"])
        self.assertEqual(stale["code"], "STALE_CLOSE")
        self.assertFalse(ws["A"].closed)
        self.assertEqual(svc.source_status()["A"]["state"], "OPEN")

        # A is usable again
        out = svc.ingestion._run("batch-534", "shard-1", deliver=True, block=True)
        self.assertEqual(out["status"], "COMPLETE")
        self.assertFalse(ws["A"].closed)

    def test_cancel_after_real_close_is_refused(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=1)
        svc.start_batch()
        svc.maintain("m1")
        self.assertTrue(ws["A"].closed)
        res = svc.registry.cancel_maintenance("A", "m1")
        self.assertFalse(res["ok"])
        self.assertIn(res["code"], ("NO_ACTIVE_MAINTENANCE", "ALREADY_CLOSED"))

    def test_close_and_cancel_cannot_interleave(self):
        root = tempfile.mkdtemp()
        entered = threading.Event()
        release = threading.Event()

        def hook(src):
            entered.set()
            release.wait(5)

        ws = build_workspace(root, num_shards=1)
        for f in ws["A"].files.values():
            f.close_hook = hook
        svc = Service(root, {"A": ws["A"], "B": ws["B"]}, ws["descriptor"])
        f = ws["A"].fixture("shard-1")
        f.allow_read.clear()
        t = svc.ingestion.dispatch("batch-534", "shard-1")
        self.assertTrue(f.before_read.wait(3))
        svc.maintain("m1")
        f.allow_read.set()
        self.assertTrue(entered.wait(5))
        result = {}

        def do_cancel():
            result.update(svc.registry.cancel_maintenance("A", "m1"))

        ct = threading.Thread(target=do_cancel)
        ct.start()
        time.sleep(0.15)
        self.assertTrue(ct.is_alive(), "cancel must wait for the in-progress close")
        release.set()
        t.join(5)
        ct.join(5)
        self.assertFalse(result.get("ok"))
        self.assertTrue(ws["A"].closed)


class Qualification(unittest.TestCase):
    def test_b_divergent_digest_is_unavailable(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=2, b_overrides={"shard-2": [("x", 999)]})
        f = ws["A"].fixture("shard-1")
        f.allow_read.clear()
        t = svc.ingestion.dispatch("batch-534", "shard-1")
        self.assertTrue(f.before_read.wait(3))
        svc.maintain("m1")

        out = svc.ingestion._run("batch-534", "shard-2", deliver=True, block=True)
        self.assertEqual(out["status"], "UNAVAILABLE")
        self.assertIn("DIGEST_MISMATCH", "".join(out["reasons"]))

        f.allow_read.set()
        t.join(5)
        st = svc.batch_status()
        self.assertEqual(st["shards"]["shard-2"]["state"], "UNAVAILABLE")
        self.assertIn("shard-2", st["incomplete"])
        self.assertNotEqual(st["state"], "COMPLETE")
        v = svc.verify()
        self.assertFalse(v["ok"])
        self.assertEqual(v["incomplete"], ["shard-2"])
        # zero rows imported for the divergent shard
        self.assertEqual(
            svc.ingestion.db.execute("SELECT COUNT(*) FROM rows WHERE shard_id='shard-2'").fetchone()[0], 0
        )

    def test_b_wrong_snapshot_not_a_candidate(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=2, b_snapshot="other-snapshot")
        f = ws["A"].fixture("shard-1")
        f.allow_read.clear()
        t = svc.ingestion.dispatch("batch-534", "shard-1")
        self.assertTrue(f.before_read.wait(3))
        svc.maintain("m1")
        # shard-2: A is draining and B declares a different snapshot, so B is
        # not a qualified replacement even though the file name matches.
        out = svc.ingestion._run("batch-534", "shard-2", deliver=True, block=True)
        self.assertEqual(out["status"], "UNAVAILABLE")
        self.assertIn("NO_QUALIFIED_SOURCE", out["reason"])
        f.allow_read.set()
        t.join(5)
        st = svc.batch_status()
        self.assertEqual(st["shards"]["shard-1"]["state"], "COMPLETE")
        self.assertEqual(st["shards"]["shard-2"]["state"], "UNAVAILABLE")
        self.assertEqual(st["incomplete"], ["shard-2"])
        # B was never read
        self.assertEqual([e for e in ws["B"].audit if e["op"] == "read-start"], [])

    def test_never_uses_b_for_completed_shard_rerun(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=2)
        svc.start_batch()
        svc.maintain("m1")
        before = svc.ingestion.db.execute("SELECT COUNT(*) FROM shards_done").fetchone()[0]
        svc.step()
        after = svc.ingestion.db.execute("SELECT COUNT(*) FROM shards_done").fetchone()[0]
        self.assertEqual(before, after)
        self.assertEqual(svc.batch_status()["state"], "COMPLETE")


class CancelBatch(unittest.TestCase):
    def test_cancel_after_read_before_callback(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=2)
        f = ws["A"].fixture("shard-1")
        f.allow_callback.clear()
        result = {}
        t = threading.Thread(
            target=lambda: result.update(svc.ingestion._run("batch-534", "shard-1", deliver=True, block=True))
        )
        t.start()
        self.assertTrue(f.before_callback.wait(5))

        c = svc.cancel_batch()
        self.assertIn("shard-1", c["cancelled"])
        self.assertEqual(svc.source_status()["A"]["in_flight"], 1)  # still physically reading

        f.allow_callback.set()
        t.join(5)
        self.assertEqual(result.get("status"), "CANCELLED")
        # no rows imported, ticket released, no duplicate import on a late callback
        self.assertEqual(svc.ingestion.db.execute("SELECT COUNT(*) FROM rows").fetchone()[0], 0)
        self.assertEqual(svc.source_status()["A"]["in_flight"], 0)
        late = svc.ingestion.deliver("batch-534", "shard-1")
        self.assertIn(late["status"], ("NO_PENDING", "CANCELLED"))
        self.assertEqual(svc.ingestion.db.execute("SELECT COUNT(*) FROM rows").fetchone()[0], 0)

    def test_cancel_aborts_unstarted_tickets(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=1)
        f = ws["A"].fixture("shard-1")
        f.allow_read.clear()
        t = svc.ingestion.dispatch("batch-534", "shard-1")
        self.assertTrue(f.before_read.wait(3))
        # a second ticket that has not started reading
        tk = svc.registry.acquire("A", "batch-534", "shard-1")
        svc.cancel_batch()
        f.allow_read.set()
        t.join(5)
        self.assertEqual(svc.source_status()["A"]["in_flight"], 0)
        self.assertIsNone(svc.registry.ticket_state(tk))


class DuplicateCompletion(unittest.TestCase):
    def test_commit_at_most_once(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=1)
        out = svc.ingestion._run("batch-534", "shard-1", deliver=False, block=True)
        rec = out["record"]
        self.assertEqual(svc.ingestion._deliver(rec)["status"], "COMPLETE")
        self.assertEqual(svc.ingestion._deliver(rec)["status"], "DUPLICATE")
        self.assertEqual(svc.ingestion.deliver("batch-534", "shard-1")["status"], "DUPLICATE")
        self.assertEqual(svc.ingestion.db.execute("SELECT COUNT(*) FROM rows").fetchone()[0], 50)
        self.assertEqual(svc.ingestion.db.execute("SELECT COUNT(*) FROM shards_done").fetchone()[0], 1)
        self.assertEqual(svc.registry.ticket_counts()["A"], 0)


class SourceReadFailure(unittest.TestCase):
    def test_read_failure_releases_and_falls_back(self):
        root = tempfile.mkdtemp()
        ws = build_workspace(root, num_shards=1)

        class Boom:
            name = "A"
            snapshot = "frozen-1"
            active = 0
            closed = False
            reads = []
            audit = []

            def read_for(self, *a):
                raise IOError("disk gone")

            def close(self):
                if self.active:
                    raise RuntimeError("ACTIVE_READS")
                self.closed = True

        svc = Service(root, {"A": Boom(), "B": ws["B"]}, ws["descriptor"])
        out = svc.ingestion._run("batch-534", "shard-1", deliver=True, block=True)
        self.assertEqual(out["status"], "COMPLETE")
        self.assertEqual(out["source"], "B")
        self.assertEqual(svc.registry.ticket_counts()["A"], 0)
        self.assertEqual(svc.registry.ticket_counts()["B"], 0)
        self.assertTrue(svc.verify()["ok"])


class ConcurrentDispatchers(unittest.TestCase):
    def test_two_dispatchers_same_shard(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=1)
        f = ws["A"].fixture("shard-1")
        f.allow_read.clear()
        t1 = svc.ingestion.dispatch("batch-534", "shard-1")
        self.assertTrue(f.before_read.wait(3))
        t2 = svc.ingestion.dispatch("batch-534", "shard-1")
        t2.join(3)
        f.allow_read.set()
        t1.join(5)
        self.assertEqual(svc.batch_status()["state"], "COMPLETE")
        self.assertEqual(len([e for e in ws["A"].audit if e["op"] == "read-start"]), 1)
        self.assertEqual(svc.ingestion.db.execute("SELECT COUNT(*) FROM rows").fetchone()[0], 50)


class ZeroRowShard(unittest.TestCase):
    def test_zero_row_shard_is_complete_not_missing(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=2, rows_per_shard=0)
        svc.start_batch()
        st = svc.batch_status()
        self.assertEqual(st["state"], "COMPLETE")
        self.assertEqual(st["incomplete"], [])
        v = svc.verify()
        self.assertTrue(v["ok"], v)
        self.assertEqual(v["origin_total"], 0)
        self.assertEqual(v["shards"]["shard-1"]["rows"], 0)
        self.assertEqual(v["shards"]["shard-1"]["row_check"], "OK")

    def test_incomplete_reports_missing_shards(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=3, rows_per_shard=5)
        f = ws["A"].fixture("shard-2")
        f.allow_read.clear()
        t = svc.ingestion.dispatch("batch-534", "shard-2")
        self.assertTrue(f.before_read.wait(3))
        svc.maintain("m1")
        v = svc.verify()
        self.assertFalse(v["ok"])
        self.assertIn("shard-2", v["incomplete"])
        f.allow_read.set()
        t.join(5)


class Descriptor(unittest.TestCase):
    def test_descriptor_frozen_and_valid(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=2, rows_per_shard=50)
        desc = ws["descriptor"]
        validate_descriptor(desc)
        self.assertEqual(desc["batch_id"], "batch-534")
        self.assertEqual(desc["snapshot"], "frozen-1")
        self.assertEqual(len(desc["shards"]), 2)
        for spec in desc["shards"]:
            self.assertEqual(spec["first_row"], 1)
            self.assertEqual(spec["last_row"], 50)
            self.assertEqual(len(spec["sha256"]), 64)
            self.assertGreater(spec["bytes"], 0)
        # descriptor file unchanged after ingestion
        before = descriptor_sha256(load_descriptor(os.path.join(root, "descriptor.json")))
        svc.start_batch()
        after = descriptor_sha256(load_descriptor(os.path.join(root, "descriptor.json")))
        self.assertEqual(before, after)

    def test_public_verify_matches_only_same_snapshot_and_digest(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=2, b_overrides={"shard-2": [("x", 1)]})
        desc = load_descriptor(os.path.join(root, "descriptor.json"))
        self.assertTrue(verify_shard_file(desc, "shard-1", ws["a_map"]["shard-1"])["ok"])
        self.assertTrue(verify_shard_file(desc, "shard-1", ws["b_map"]["shard-1"])["ok"])
        bad = verify_shard_file(desc, "shard-2", ws["b_map"]["shard-2"])
        self.assertFalse(bad["ok"])
        self.assertFalse(bad["sha256_match"])
        wrong_snap = verify_shard_file(desc, "shard-1", ws["b_map"]["shard-1"], source_snapshot="other")
        self.assertFalse(wrong_snap["ok"])
        self.assertFalse(wrong_snap["snapshot_match"])


class KeptWaiting(unittest.TestCase):
    def test_unavailable_is_retried_not_completed(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=1, b_overrides={"shard-1": [("x", 7)]})
        # A is closed -> B must serve, but B digest differs -> UNAVAILABLE
        svc.maintain("m1")
        self.assertTrue(ws["A"].closed)
        out = svc.step()
        self.assertEqual(out["status"], "UNAVAILABLE")
        d = svc.step()
        self.assertEqual(d["status"], "UNAVAILABLE")
        self.assertEqual(svc.batch_status()["shards"]["shard-1"]["state"], "UNAVAILABLE")
        self.assertEqual(svc.batch_status()["state"], "BLOCKED")

        # Repair B's real file to the descriptor bytes: a later step now succeeds.
        import shutil
        shutil.copyfile(ws["a_map"]["shard-1"], ws["b_map"]["shard-1"])
        out = svc.step()
        self.assertEqual(out["status"], "COMPLETE")
        self.assertEqual(svc.batch_status()["state"], "COMPLETE")
        self.assertTrue(svc.verify()["ok"])


class ShardIdentity(unittest.TestCase):
    def test_identical_csv_content_different_shards_processed_separately(self):
        root = tempfile.mkdtemp()
        # Two shards with byte-identical content would collide if keyed by content
        # or file name only; they must be processed as distinct shards.
        os.makedirs(os.path.join(root, "sources", "A"), exist_ok=True)
        os.makedirs(os.path.join(root, "sources", "B"), exist_ok=True)
        same_rows = [("same", 5), ("other", 9)]
        files = {}
        for sid in ("shard-1", "shard-2"):
            p = os.path.join(root, "sources", "A", f"{sid}.csv")
            _write_csv(p, same_rows)
            files[sid] = p
            import shutil
            shutil.copyfile(p, os.path.join(root, "sources", "B", f"{sid}.csv"))
        desc = build_descriptor("dup-batch", "frozen-1", files, origin_dir=root)
        self.assertEqual(desc["shards"][0]["sha256"], desc["shards"][1]["sha256"])
        a = CompositeSource("A", files, snapshot="frozen-1")
        b = CompositeSource("B", {sid: os.path.join(root, "sources", "B", f"{sid}.csv") for sid in files})
        svc = Service(root, {"A": a, "B": b}, desc)
        svc.start_batch()
        st = svc.batch_status()
        self.assertEqual(st["state"], "COMPLETE")
        self.assertEqual(st["rows"], 4)
        # row identity is (shard_id, row_no), so both shards kept both rows
        for sid in ("shard-1", "shard-2"):
            n = svc.ingestion.db.execute(
                "SELECT COUNT(*) FROM rows WHERE batch_id='dup-batch' AND shard_id=?", (sid,)
            ).fetchone()[0]
            self.assertEqual(n, 2)
        self.assertTrue(svc.verify()["ok"])


class AcquiredBeforeMaintenance(unittest.TestCase):
    def test_acquired_ticket_is_in_flight_and_drains_not_redispatched(self):
        """A ticket granted before maintenance is legal work.

        Even though no bytes have arrived yet, that shard must complete on A and
        must not be handed to B.  Only a shard with no ticket yet is redirected.
        """
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=2)
        A = ws["A"]
        f1 = A.fixture("shard-1")
        # Pause *after* read-start increments but before bytes are returned is a
        # callback hold; instead pause before file open, which is exactly the
        # "ticket acquired, not yet read" in-flight window.
        f1.allow_read.clear()
        t = svc.ingestion.dispatch("batch-534", "shard-1")
        self.assertTrue(f1.before_read.wait(3))

        m = svc.maintain("m1")
        self.assertEqual(m["status"], "ACTIVE")
        self.assertEqual(len(m["in_flight"]), 1)  # adopted, not aborted
        self.assertEqual(m["aborted_tickets"], [])

        # shard-2 has no ticket -> redirected to B
        out2 = svc.ingestion._run("batch-534", "shard-2", deliver=True, block=True)
        self.assertEqual(out2["source"], "B")

        # release shard-1 -> complete on A, then A really closes
        f1.allow_read.set()
        t.join(5)
        st = svc.batch_status()
        self.assertEqual(st["state"], "COMPLETE")
        self.assertEqual(st["shards"]["shard-1"]["source"], "A")
        self.assertEqual(st["shards"]["shard-2"]["source"], "B")
        self.assertTrue(A.closed)
        self.assertTrue(svc.verify()["ok"])

    def test_acquire_after_maintenance_is_refused(self):
        root = tempfile.mkdtemp()
        ws, svc = make(root, num_shards=1)
        svc.maintain("m1", )
        # maintenance with no in-flight work closes immediately; a post-close
        # acquire must be refused outright.
        with self.assertRaises(Exception) as ctx:
            svc.registry.acquire("A", "batch-534", "shard-1")
        self.assertEqual(getattr(ctx.exception, "code", None), "SOURCE_CLOSED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
