"""Frozen batch descriptors.

A descriptor is the only public description of a batch.  It freezes the
batch_id, the logical snapshot id and, for every logical shard, the exact
byte count, SHA-256 digest and the 1-based inclusive range of CSV data rows.
Nothing in this module ever rewrites a descriptor; source replicas are only
ever *checked against* it.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

FORMAT = "batch-descriptor/1"


class DescriptorError(ValueError):
    """Descriptor or CSV violates the frozen format."""


def natural_key(value: str):
    """sort shard-2 before shard-10 instead of lexicographically."""
    return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", value)]


def parse_csv_bytes(raw: bytes) -> Tuple[List[str], List[dict]]:
    """Parse the fixture CSV format and return (header, data rows).

    Blank physical lines are ignored; every produced row has a 1-based
    ``row_no`` plus its ``row_id`` and integer ``value``.
    """
    if not raw:
        raise DescriptorError("EMPTY_FILE")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:  # pragma: no cover - fixture guard
        raise DescriptorError(f"NOT_UTF8:{exc}") from exc
    parsed = list(csv.reader(io.StringIO(text)))
    parsed = [row for row in parsed if row]  # blank line is not a data row
    if not parsed:
        raise DescriptorError("NO_HEADER")
    header = parsed[0]
    if "value" not in header:
        raise DescriptorError("MISSING_VALUE_COLUMN")
    value_idx = header.index("value")
    row_id_idx = header.index("row_id") if "row_id" in header else None
    rows: List[dict] = []
    for row_no, row in enumerate(parsed[1:], start=1):
        if len(row) <= value_idx:
            raise DescriptorError(f"SHORT_ROW:{row_no}")
        try:
            value = int(row[value_idx])
        except ValueError as exc:
            raise DescriptorError(f"BAD_INTEGER:{row_no}:{row[value_idx]!r}") from exc
        row_id = row[row_id_idx] if row_id_idx is not None and row_id_idx < len(row) else str(row_no)
        rows.append({"row_no": row_no, "row_id": row_id, "value": value})
    return header, rows


def build_descriptor(
    batch_id: str,
    snapshot: str,
    shard_files: Mapping[str, str],
    origin_dir: Optional[str] = None,
    primary: str = "A",
    backups: Optional[Iterable[str]] = None,
) -> dict:
    """Build a descriptor from the authoritative (unaltered) files."""
    specs = []
    for shard_id in sorted(shard_files, key=natural_key):
        path = shard_files[shard_id]
        with open(path, "rb") as fh:
            raw = fh.read()
        header, rows = parse_csv_bytes(raw)
        origin = os.path.relpath(path, origin_dir) if origin_dir else path
        specs.append(
            {
                "shard_id": shard_id,
                "bytes": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest(),
                "snapshot": snapshot,
                "first_row": 1,
                "last_row": len(rows),
                "row_count": len(rows),
                "columns": header,
                "origin": origin,
            }
        )
    return {
        "format": FORMAT,
        "batch_id": batch_id,
        "snapshot": snapshot,
        "primary": primary,
        "backups": list(backups if backups is not None else ["B"]),
        "shards": specs,
    }


def validate_descriptor(desc: dict) -> dict:
    """Strict validation.  Returns the descriptor for chaining."""
    if desc.get("format") != FORMAT:
        raise DescriptorError(f"BAD_FORMAT:{desc.get('format')!r}")
    if not desc.get("batch_id"):
        raise DescriptorError("MISSING_BATCH_ID")
    if not desc.get("snapshot"):
        raise DescriptorError("MISSING_SNAPSHOT")
    shards = desc.get("shards")
    if not isinstance(shards, list) or not shards:
        raise DescriptorError("MISSING_SHARDS")
    seen = set()
    for spec in shards:
        sid = spec.get("shard_id")
        if not sid:
            raise DescriptorError("MISSING_SHARD_ID")
        if sid in seen:
            raise DescriptorError(f"DUPLICATE_SHARD:{sid}")
        seen.add(sid)
        for field in ("bytes", "row_count", "first_row", "last_row"):
            if not isinstance(spec.get(field), int):
                raise DescriptorError(f"BAD_{field.upper()}:{sid}")
        if spec["bytes"] < 0 or spec["row_count"] < 0:
            raise DescriptorError(f"NEGATIVE_SIZE:{sid}")
        if spec["row_count"] == 0:
            if spec["first_row"] != 1 or spec["last_row"] != 0:
                raise DescriptorError(f"BAD_EMPTY_RANGE:{sid}")
        else:
            if spec["first_row"] != 1 or spec["last_row"] != spec["row_count"]:
                raise DescriptorError(f"BAD_RANGE:{sid}")
        sha = spec.get("sha256", "")
        if not isinstance(sha, str) or len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha):
            raise DescriptorError(f"BAD_SHA256:{sid}")
        if spec.get("snapshot") != desc["snapshot"]:
            raise DescriptorError(f"SHARD_SNAPSHOT_MISMATCH:{sid}")
    return desc


def shard_map(desc: dict) -> Dict[str, dict]:
    return {spec["shard_id"]: spec for spec in desc["shards"]}


def canonical_json(desc: dict) -> str:
    return json.dumps(desc, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def descriptor_sha256(desc: dict) -> str:
    return hashlib.sha256(canonical_json(desc).encode("utf-8")).hexdigest()


def load_descriptor(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return validate_descriptor(json.load(fh))


def save_descriptor(desc: dict, path: str) -> None:
    validate_descriptor(desc)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(desc, fh, indent=2, sort_keys=True, ensure_ascii=False)
        fh.write("\n")
    os.replace(tmp, path)


def verify_shard_file(desc: dict, shard_id: str, path: str, source_snapshot: Optional[str] = None) -> dict:
    """Publicly verify that a real file is the descriptor's shard.

    This is the same check any replica (A or B) must pass before it may serve
    a shard: the snapshot must match and the bytes/digest/row-range must equal
    the frozen descriptor exactly.  A matching file name alone is not enough.
    """
    specs = shard_map(desc)
    if shard_id not in specs:
        raise DescriptorError(f"UNKNOWN_SHARD:{shard_id}")
    spec = specs[shard_id]
    with open(path, "rb") as fh:
        raw = fh.read()
    digest = hashlib.sha256(raw).hexdigest()
    snapshot = source_snapshot if source_snapshot is not None else spec["snapshot"]
    try:
        _, rows = parse_csv_bytes(raw)
        parse_error = None
    except DescriptorError as exc:
        rows = []
        parse_error = str(exc)
    row_nos = [r["row_no"] for r in rows]
    row_range_ok = (
        parse_error is None
        and len(rows) == spec["row_count"]
        and (row_nos == list(range(spec["first_row"], spec["last_row"] + 1)) if rows else spec["row_count"] == 0)
    )
    snapshot_ok = snapshot == spec["snapshot"]
    size_ok = len(raw) == spec["bytes"]
    digest_ok = digest == spec["sha256"]
    ok = snapshot_ok and size_ok and digest_ok and row_range_ok
    return {
        "shard_id": shard_id,
        "file": path,
        "ok": ok,
        "snapshot_match": snapshot_ok,
        "size_match": size_ok,
        "sha256_match": digest_ok,
        "row_range_match": row_range_ok,
        "expected": {
            "snapshot": spec["snapshot"],
            "bytes": spec["bytes"],
            "sha256": spec["sha256"],
            "row_range": [spec["first_row"], spec["last_row"]],
        },
        "actual": {
            "snapshot": snapshot,
            "bytes": len(raw),
            "sha256": digest,
            "row_count": len(rows) if parse_error is None else None,
            "parse_error": parse_error,
        },
    }
