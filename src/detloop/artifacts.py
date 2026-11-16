"""Replay manifests, safe artifact writes, and the exploration SQLite database."""

from __future__ import annotations

import contextlib
import datetime as _dt
import json
import os
import re
import sqlite3
import tempfile
from typing import Any

from ._compat import runtime_identity
from ._version import DB_SCHEMA_VERSION, MANIFEST_SCHEMA, TRACE_SCHEMA, __version__
from .config import SimConfig
from .errors import ConfigError, ReplayMismatch
from .runner import RunResult

_SAFE = re.compile(r"[^A-Za-z0-9_.-]+")


def safe_name(text: str, limit: int = 80) -> str:
    name = _SAFE.sub("_", text).strip("._") or "test"
    return name[:limit]


def atomic_write(path: str, data: str | bytes) -> None:
    """Write via a temporary file in the same directory and ``os.replace``."""
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    mode = "wb" if isinstance(data, bytes) else "w"
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=os.path.basename(path))
    try:
        kwargs: dict[str, Any] = (
            {} if isinstance(data, bytes) else {"encoding": "utf-8", "newline": "\n"}
        )
        with os.fdopen(fd, mode, **kwargs) as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def utc_now() -> str:
    return _dt.datetime.now(_dt.UTC).replace(microsecond=0).isoformat()


def build_manifest(
    *,
    target: str,
    test: str,
    workload_fingerprint: str,
    config: SimConfig,
    result: RunResult,
    trace_file: str | None,
    faults_explicit: bool,
    include_decisions: bool,
    shrink: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema": MANIFEST_SCHEMA,
        "trace_schema": TRACE_SCHEMA,
        "target": {"location": target, "test": test, "workload_fingerprint": workload_fingerprint},
        "seed": result.seed,
        "config": config.to_dict(),
        "config_fingerprint": config.fingerprint(),
        "scheduler": config.scheduler,
        "faults": result.faults,
        "faults_explicit": faults_explicit,
        "decisions": {"mode": "strict", "tapes": result.tapes} if include_decisions else None,
        "outcome": {
            "status": result.status,
            "kind": result.kind,
            "signature": result.signature,
            "message": result.message,
            "details": result.details,
        },
        "trace": {
            "file": trace_file,
            "sha256": result.trace_sha256,
            "events": result.trace_events,
            "truncated": result.trace_truncated,
        },
        "stats": {
            "steps": result.steps,
            "virtual_ns": result.virtual_ns,
            "decisions": result.decisions,
            "history": result.history,
            "network": result.network,
        },
        "shrink": shrink,
        "metadata": {
            **runtime_identity(),
            "detloop": __version__,
            "hash_seed": os.environ.get("PYTHONHASHSEED"),
            "created_at": utc_now(),
            "wall_seconds": round(result.wall_seconds, 6),
            "loop_errors": result.loop_errors,
            "warnings": result.warnings,
        },
    }


def write_run_artifacts(
    directory: str, manifest: dict[str, Any], trace_lines: list[str] | None
) -> str:
    """Write ``trace.jsonl`` and ``manifest.json`` into ``directory``; return the manifest path."""
    os.makedirs(directory, exist_ok=True)
    if trace_lines is not None:
        atomic_write(
            os.path.join(directory, "trace.jsonl"), "".join(line + "\n" for line in trace_lines)
        )
        manifest["trace"]["file"] = "trace.jsonl"
    path = os.path.join(directory, "manifest.json")
    atomic_write(path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return path


def read_manifest(path: str) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read manifest {path}: {exc}") from None
    if not isinstance(data, dict) or data.get("schema") != MANIFEST_SCHEMA:
        raise ReplayMismatch(
            f"{path} is not a {MANIFEST_SCHEMA} manifest (schema={data.get('schema')!r})"
        )
    return data


def manifest_config(manifest: dict[str, Any]) -> SimConfig:
    cfg = SimConfig.from_dict(manifest["config"])
    if cfg.fingerprint() != manifest.get("config_fingerprint"):
        raise ReplayMismatch(
            "manifest config does not match its recorded fingerprint "
            "(edited manifest or incompatible detloop version)"
        )
    return cfg


# ----------------------------------------------------------------------------
# SQLite aggregation

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS explorations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target TEXT NOT NULL,
    test TEXT NOT NULL,
    workload_fingerprint TEXT,
    config_json TEXT NOT NULL,
    config_fingerprint TEXT NOT NULL,
    seed_start INTEGER NOT NULL,
    seed_count INTEGER NOT NULL,
    jobs INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    detloop_version TEXT NOT NULL,
    python TEXT NOT NULL,
    platform TEXT NOT NULL,
    hash_seed TEXT
);
CREATE TABLE IF NOT EXISTS runs (
    exploration_id INTEGER NOT NULL REFERENCES explorations(id),
    seed INTEGER NOT NULL,
    status TEXT NOT NULL,
    kind TEXT NOT NULL,
    signature TEXT NOT NULL,
    message TEXT NOT NULL,
    steps INTEGER NOT NULL,
    virtual_ns INTEGER NOT NULL,
    trace_sha256 TEXT NOT NULL,
    trace_events INTEGER NOT NULL,
    faults INTEGER NOT NULL,
    decisions INTEGER NOT NULL,
    wall_seconds REAL NOT NULL,
    completion_order INTEGER NOT NULL,
    artifact TEXT,
    PRIMARY KEY (exploration_id, seed)
);
CREATE INDEX IF NOT EXISTS runs_status ON runs(exploration_id, status);
"""


class ResultsDB:
    """Versioned SQLite store for exploration results (single writer: the parent)."""

    def __init__(self, path: str) -> None:
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        version = self._version()
        if version is None:
            self.conn.executescript(_SCHEMA)
            self.conn.execute(
                "INSERT OR REPLACE INTO meta VALUES ('schema_version', ?)",
                (str(DB_SCHEMA_VERSION),),
            )
            self.conn.commit()
        elif version != DB_SCHEMA_VERSION:
            self.conn.close()
            raise ConfigError(
                f"{path} uses detloop results schema v{version}; "
                f"this detloop writes v{DB_SCHEMA_VERSION}. "
                "Use a new --db path."
            )

    def _version(self) -> int | None:
        try:
            row = self.conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        except sqlite3.OperationalError:
            return None
        return int(row[0]) if row else None

    def start(
        self,
        *,
        target: str,
        test: str,
        fingerprint: str,
        config: SimConfig,
        seeds: range,
        jobs: int,
    ) -> int:
        ident = runtime_identity()
        cur = self.conn.execute(
            "INSERT INTO explorations (target, test, workload_fingerprint, config_json, "
            "config_fingerprint, seed_start, seed_count, jobs, started_at, status, "
            "detloop_version, python, platform, hash_seed) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                target,
                test,
                fingerprint,
                json.dumps(config.to_dict(), sort_keys=True),
                config.fingerprint(),
                seeds.start,
                len(seeds),
                jobs,
                utc_now(),
                "running",
                __version__,
                ident["python"],
                ident["platform"],
                os.environ.get("PYTHONHASHSEED"),
            ),
        )
        self.conn.commit()
        return int(cur.lastrowid or 0)

    def add_run(
        self, exploration_id: int, summary: dict[str, Any], order: int, artifact: str | None
    ) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                exploration_id,
                summary["seed"],
                summary["status"],
                summary["kind"],
                summary["signature"],
                summary["message"][:2000],
                summary["steps"],
                int(summary["virtual_time"] * 1e9),
                summary["trace_sha256"],
                summary["trace_events"],
                summary["faults"],
                summary["decisions"],
                summary["wall_seconds"],
                order,
                artifact,
            ),
        )

    def finish(self, exploration_id: int, status: str) -> None:
        self.conn.execute(
            "UPDATE explorations SET finished_at=?, status=? WHERE id=?",
            (utc_now(), status, exploration_id),
        )
        self.conn.commit()

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def load_db(path: str) -> dict[str, Any]:
    """Read an exploration database for reporting (latest exploration first)."""
    if not os.path.isfile(path):
        raise ConfigError(f"results database not found: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if row is None or int(row[0]) != DB_SCHEMA_VERSION:
            raise ConfigError(f"{path}: unsupported results schema")
        explorations = [
            dict(r) for r in conn.execute("SELECT * FROM explorations ORDER BY id DESC")
        ]
        for ex in explorations:
            ex["runs"] = [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM runs WHERE exploration_id=? ORDER BY seed", (ex["id"],)
                )
            ]
        return {"path": path, "explorations": explorations}
    finally:
        conn.close()
