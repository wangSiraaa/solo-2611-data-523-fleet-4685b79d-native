# Finite standby protocol

Run `python3 probe.py` in this directory. `db.py` is a SQLite-backed row-version store. `Replay` is used by a replication client; `Readers` has an independent connection and is used by interactive report clients. No external server is required.

The initial implementation supports ordinary committed single transactions and paged reads. `Replay.shutdown()` / `Readers.shutdown()` close their connections. A new instance with the same path reopens the database. Time is an explicit logical-millisecond argument, allowing repeatable conflict schedules.

`versions` stores inclusive `born` and exclusive `dead` LSNs. A tombstone has `deleted=1`. The target protocol and API behavior are specified in the task. You may change the schema and add state, but retain callable service entry points.
