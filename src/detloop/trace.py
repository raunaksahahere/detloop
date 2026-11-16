"""Canonical JSONL trace recording.

Each event is serialized immediately with ``sort_keys=True``,
``separators=(",", ":")`` and ``ensure_ascii=True``; times are integer
nanoseconds of virtual time. The SHA-256 of the canonical bytes (each line
followed by ``\\n``) is the run's *trace hash*. Nothing that depends on the
host (wall-clock time, PIDs, object addresses, absolute paths, Python version)
may enter canonical events; such data goes into the manifest's ``metadata``.
"""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import re
import sys
from collections.abc import Iterator
from typing import Any

from ._version import TRACE_SCHEMA

_ADDR_RE = re.compile(r" at 0x[0-9a-fA-F]+")
_ERRNO_RE = re.compile(r"\[Errno (\d+)\]")


def _errno_name(m: re.Match[str]) -> str:
    # errno numbers are host-specific: ECONNREFUSED is 111 on Linux, 61 on macOS
    # and 10061 (named WSAECONNREFUSED) on Windows.
    name = errno.errorcode.get(int(m.group(1)), m.group(1))
    if name.startswith("WSAE"):
        name = name[3:]
    return f"[Errno {name}]"


_MAX_TEXT = 2000

LEVELS = {"off": 0, "basic": 1, "full": 2}
_RESERVED = frozenset({"i", "k", "t"})


def _host_prefixes() -> list[tuple[str, str]]:
    """Host-specific directory prefixes replaced in trace text (longest first)."""
    import site
    import sysconfig

    found: dict[str, str] = {}
    for key in ("stdlib", "platstdlib", "purelib", "platlib"):
        path = sysconfig.get_paths().get(key)
        if path:
            found.setdefault(path, f"<{key}>")
    try:
        for path in site.getsitepackages():
            found.setdefault(path, "<site-packages>")
    except AttributeError:  # pragma: no cover - virtualenv without site API
        pass
    for path, token in (
        (sys.prefix, "<prefix>"),
        (sys.base_prefix, "<base-prefix>"),
        (os.getcwd(), "<cwd>"),
        (os.path.expanduser("~"), "<home>"),
    ):
        if path and len(path) > 1:
            found.setdefault(path, token)
    pairs = [(p, t) for p, t in found.items() if len(p) > 3]
    return sorted(pairs, key=lambda pt: -len(pt[0]))


_PREFIXES: list[tuple[str, str]] | None = None


def sanitize_text(text: str) -> str:
    """Remove object addresses and host paths, and bound the length of trace text."""
    global _PREFIXES
    text = _ADDR_RE.sub(" at 0x?", text)
    if "[Errno " in text:
        text = _ERRNO_RE.sub(_errno_name, text)
    if _PREFIXES is None:
        _PREFIXES = _host_prefixes()
    for prefix, token in _PREFIXES:
        if prefix in text:
            text = text.replace(prefix, token)
    if len(text) > _MAX_TEXT:
        text = text[:_MAX_TEXT] + f"...[truncated {len(text) - _MAX_TEXT} chars]"
    return text


def canonical_json(obj: Any) -> str:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    )


def jsonable(value: Any, depth: int = 0) -> Any:
    """Convert a user value into canonical JSON data (deterministically)."""
    if value is None or isinstance(value, bool | int | str):
        return sanitize_text(value) if isinstance(value, str) else value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return repr(value)
        return value
    if depth > 6:
        return sanitize_text(repr(value))
    if isinstance(value, bytes | bytearray):
        return {"bytes": bytes(value[:256]).hex(), "len": len(value)}
    if isinstance(value, list | tuple):
        return [jsonable(v, depth + 1) for v in value]
    if isinstance(value, dict):
        return {
            str(k): jsonable(v, depth + 1)
            for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))
        }
    if isinstance(value, set | frozenset):
        items = [jsonable(v, depth + 1) for v in value]
        return sorted(items, key=canonical_json)
    if isinstance(value, BaseException):
        return {"error": type(value).__qualname__, "message": sanitize_text(str(value))}
    return sanitize_text(repr(value))


class Trace:
    """Bounded canonical event recorder with an incremental hash."""

    def __init__(self, level: str = "basic", max_events: int = 200_000) -> None:
        self.level_name = level
        self.level = LEVELS[level]
        self.max_events = max_events
        self.lines: list[str] = []
        self.count = 0
        self.truncated = False
        self._hash = hashlib.sha256()
        self._clock: Any = None
        self.sealed = False

    def bind_clock(self, clock: Any) -> None:
        """``clock`` is a zero-argument callable returning virtual time in ns."""
        self._clock = clock

    def wants(self, level: int) -> bool:
        return self.level >= level

    def emit(self, kind: str, level: int = 1, /, **fields: Any) -> int:
        """Record an event if the trace level includes ``level``. Returns its id."""
        if self.level < level or self.sealed:
            return -1
        eid = self.count
        self.count += 1
        if len(self.lines) >= self.max_events:
            if not self.truncated:
                self.truncated = True
                self._add({"i": eid, "k": "trace_truncated", "t": self._now()})
            return eid
        event = {"i": eid, "k": kind, "t": self._now()}
        for key, value in fields.items():
            if value is not None:
                event[f"{key}_" if key in _RESERVED else key] = jsonable(value)
        self._add(event)
        return eid

    def header(self, **fields: Any) -> None:
        event = {"i": self.count, "k": "header", "schema": TRACE_SCHEMA, "t": 0}
        self.count += 1
        for key, value in fields.items():
            event[key] = jsonable(value)
        self._add(event)

    def _now(self) -> int:
        return int(self._clock()) if self._clock is not None else 0

    def _add(self, event: dict[str, Any]) -> None:
        line = canonical_json(event)
        self.lines.append(line)
        self._hash.update(line.encode("ascii"))
        self._hash.update(b"\n")

    def seal(self) -> None:
        """Stop recording (called after the ``end`` event; teardown is not traced)."""
        self.sealed = True

    def sha256(self) -> str:
        return self._hash.hexdigest()

    def iter_events(self) -> Iterator[dict[str, Any]]:
        for line in self.lines:
            yield json.loads(line)

    def text(self) -> str:
        return "".join(line + "\n" for line in self.lines)


def read_trace(path: str) -> list[dict[str, Any]]:
    out = []
    with open(path, encoding="ascii") as fh:
        for n, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{n}: invalid trace line: {exc}") from None
    return out


def hash_trace_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for line in fh:
            h.update(line.rstrip(b"\r\n"))
            h.update(b"\n")
    return h.hexdigest()
