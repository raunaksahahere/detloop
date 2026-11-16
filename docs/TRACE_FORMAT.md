# Trace, manifest and results formats

All formats are versioned. A reader must reject a schema it does not know.

## Canonical trace: `detloop-trace/1` (JSONL)

One JSON object per line, serialized with sorted keys, `(",", ":")`
separators, ASCII only, and no NaN/Infinity. The **trace hash** is the
SHA-256 of all lines, each followed by `\n`.

Common fields of every event:

| Field | Meaning |
|---|---|
| `i` | Event id, 0-based, dense, in emission order |
| `k` | Kind |
| `t` | Virtual time in integer nanoseconds |

User-supplied fields named `i`, `k` or `t` (e.g. through `world.note`) are
renamed `i_`, `k_`, `t_`.

### Identities

- Nodes: `n1` (configuration) or `n1#2` (incarnation 2); `control`.
- Connections: `c1`, `c2`, ... per run, with `side` `client`/`server`.
- Tasks (full level): integer ids per run.
- Operations: integer ids in `world.history`; faults: `f0`, `f1`, ...;
  messages: integer ids.

### Event kinds

| Level | Kinds |
|---|---|
| header (always) | `header`: `schema`, `seed`, `config` (fingerprint), `scheduler`, `nodes`, `faults` (schedule) |
| basic | `node_start`, `node_crash` (`frozen_tasks`), `node_exit`, `node_error` (`error` type); `listen`, `unlisten`; `connect`, `accept`, `established`, `refused`, `connect_timeout`, `close`, `eof`, `abort`, `rst`, `stall`, `retransmit`, `timeout`; `msg_send`, `msg_deliver`, `msg_drop` (`reason`); `partition`, `heal`, `reset`, `latency`, `latency_end`, `clock`; `fault`, `fault_end`, `fault_skipped`; `invoke`, `complete`; `invariant_failed`, `check`, `nondeterminism`, `loop_error`; `note`; `end` (`status`, `signature`) |
| full (adds) | `choose` (scheduler decision: `n`, `pick`, `of`), `task`, `task_done`, `send`/`deliver` per segment, `syn_lost`, `rst_closed` |
| off | header, `end` and nothing else |

`trace_truncated` is emitted once if `max_trace_events` is reached; the
truncation is reported in the manifest. The trace is sealed at `end`;
teardown is never traced. The trace level never changes execution.

### What is not in the trace

Wall-clock time, PIDs, object addresses (replaced by `0x?`), host paths
(replaced by `<stdlib>`, `<site-packages>`, `<prefix>`, `<cwd>`, `<home>`),
Python/OS versions, and messages of exceptions raised by user code.

## Replay manifest: `detloop-manifest/1` (JSON)

```json
{
  "schema": "detloop-manifest/1",
  "trace_schema": "detloop-trace/1",
  "target": {"location": "tests/sim_test.py", "test": "test_x", "workload_fingerprint": "<sha256>"},
  "seed": 17,
  "config": {"...": "full SimConfig"},
  "config_fingerprint": "<16 hex>",
  "scheduler": "interleave",
  "faults": [{"id": "f0", "kind": "crash", "at": 2357000000, "duration": 2007000000, "params": {"node": "n1"}}],
  "faults_explicit": false,
  "decisions": null,
  "outcome": {"status": "fail", "kind": "invariant", "signature": "invariant:x", "message": "...", "details": []},
  "trace": {"file": "trace.jsonl", "sha256": "<sha256>", "events": 2212, "truncated": false},
  "stats": {"steps": 7306, "virtual_ns": 6507028688, "decisions": 2952, "history": {}, "network": {}},
  "shrink": null,
  "metadata": {"detloop": "0.1.0", "python": "3.12.3", "platform": "linux", "machine": "x86_64",
               "implementation": "cpython", "hash_seed": "0", "created_at": "...", "wall_seconds": 0.14,
               "loop_errors": [], "warnings": []}
}
```

- `faults_explicit: true` means the schedule must be imposed rather than
  regenerated (shrunk runs).
- `decisions` holds `{"mode": "strict", "tapes": {"sched": {"bounds": [...],
  "values": [...]}, "net": ..., "workload": ...}}` when decisions are needed
  for replay (shrunk runs).
- `shrink` holds the metrics of a shrunk run.
- Everything under `metadata` is informational and non-canonical.

Mismatch handling is described in [DETERMINISM.md](DETERMINISM.md#the-replay-envelope).

## Results database: schema version 1 (SQLite)

```sql
meta(key TEXT PRIMARY KEY, value TEXT)          -- schema_version = '1'
explorations(id, target, test, workload_fingerprint, config_json, config_fingerprint,
             seed_start, seed_count, jobs, started_at, finished_at, status,
             detloop_version, python, platform, hash_seed)
runs(exploration_id, seed, status, kind, signature, message, steps, virtual_ns,
     trace_sha256, trace_events, faults, decisions, wall_seconds,
     completion_order, artifact,  PRIMARY KEY (exploration_id, seed))
```

Only the exploring parent process writes the database (WAL mode).
`completion_order` records discovery order (non-deterministic metadata);
queries should order by `seed`. Opening a database with another schema
version fails with an instruction to use a new path.

## Compatibility policy

- Adding optional fields or event kinds is backward compatible within a
  schema version; readers must ignore unknown fields and kinds.
- Removing or renaming fields, changing canonical serialization, the PRNG
  derivation or decision semantics requires a new schema / PRNG version.
- Manifests from another detloop version replay with a warning; the trace hash
  comparison then tells you whether the behaviour is identical.

## Artifact writes

Artifacts are written atomically (temporary file in the same directory,
then `os.replace`) under `<artifacts>/<sanitized test name>/seed-<N>[-shrunk]/`.
Directory names are restricted to `[A-Za-z0-9_.-]`. Trace size is bounded by
`max_trace_events` (default 200,000).
