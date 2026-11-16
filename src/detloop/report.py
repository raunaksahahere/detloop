"""Self-contained offline HTML reports.

Security model: the report embeds trace data as JSON inside a
``<script type="application/json">`` element with ``<``, ``>``, ``&`` and the
line/paragraph separators escaped, so no trace content can terminate the
element. The page's own script and stylesheet are inline and pinned by a
``Content-Security-Policy`` with SHA-256 hashes; ``default-src 'none'`` blocks
every external fetch (no CDNs, fonts or images). Rendering uses
``textContent``/SVG text nodes only.
"""

from __future__ import annotations

import base64
import hashlib
import html
import json
import os
from importlib import resources
from typing import Any

from ._shell import quote
from ._version import __version__
from .artifacts import atomic_write, load_db, read_manifest
from .errors import ConfigError
from .trace import read_trace

_LS, _PS = chr(0x2028), chr(0x2029)  # JS line/paragraph separators
#: Embedded events are capped to keep reports well below browser limits.
MAX_EMBEDDED_EVENTS = 100_000


def _asset(name: str) -> str:
    return resources.files("detloop").joinpath("report_assets", name).read_text(encoding="utf-8")


def embed_json(data: Any) -> str:
    """Serialize ``data`` so it is inert inside a ``<script>`` element."""
    text = json.dumps(data, sort_keys=True, ensure_ascii=False, allow_nan=False)
    return (
        text.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace(_LS, "\\u2028")
        .replace(_PS, "\\u2029")
    )


def _csp_hash(text: str) -> str:
    return (
        "'sha256-" + base64.b64encode(hashlib.sha256(text.encode("utf-8")).digest()).decode() + "'"
    )


def build_html(data: dict[str, Any], title: str) -> str:
    script = _asset("report.js")
    style = _asset("report.css")
    csp = (
        "default-src 'none'; "
        f"script-src {_csp_hash(script)}; "
        f"style-src {_csp_hash(style)}; "
        "img-src 'none'; connect-src 'none'; form-action 'none'; base-uri 'none'"
    )
    return (
        "<!doctype html>\n"
        '<html lang="en">\n<head>\n<meta charset="utf-8">\n'
        f'<meta http-equiv="Content-Security-Policy" content="{html.escape(csp, quote=True)}">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f'<meta name="generator" content="detloop {html.escape(__version__)}">\n'
        f"<title>{html.escape(title)}</title>\n"
        f"<style>{style}</style>\n</head>\n<body>\n"
        f'<header><h1>{html.escape(title)}</h1><div id="head"></div></header>\n'
        '<main id="app"></main>\n'
        f'<script type="application/json" id="detloop-data">{embed_json(data)}</script>\n'
        f"<script>{script}</script>\n</body>\n</html>\n"
    )


def _run_data(
    manifest: dict[str, Any] | None, events: list[dict[str, Any]], manifest_path: str | None
) -> dict[str, Any]:
    truncated = len(events) > MAX_EMBEDDED_EVENTS
    nodes: list[str] = []
    if events and events[0].get("k") == "header":
        nodes = list(events[0].get("nodes", []))
    replay = []
    if manifest_path:
        replay.append(f"detloop replay {quote(manifest_path)}")
    if manifest:
        t = manifest.get("target", {})
        if not manifest.get("decisions") and not manifest.get("faults_explicit"):
            replay.append(
                f"pytest {quote(str(t.get('location')) + '::' + str(t.get('test')))} "
                f"--detloop-seed={manifest.get('seed')}"
            )
        elif manifest_path:
            replay.append(
                f"pytest {quote(str(t.get('location')))} --detloop-replay={quote(manifest_path)}"
            )
    return {
        "kind": "run",
        "manifest": manifest or {},
        "events": events[:MAX_EMBEDDED_EVENTS],
        "truncated": truncated,
        "nodes": nodes,
        "replay": replay,
    }


def render_report(input_path: str, output_path: str) -> str:
    """Render ``input_path`` (manifest, trace JSONL or results SQLite) to ``output_path``."""
    if not os.path.exists(input_path):
        raise ConfigError(f"report input not found: {input_path}")
    lower = input_path.lower()
    if lower.endswith((".sqlite", ".db", ".sqlite3")):
        data: dict[str, Any] = {"kind": "exploration", **load_db(input_path)}
        title = "detloop exploration report"
    elif lower.endswith(".json"):
        manifest = read_manifest(input_path)
        trace_file = manifest.get("trace", {}).get("file")
        events: list[dict[str, Any]] = []
        if trace_file:
            tpath = os.path.join(os.path.dirname(os.path.abspath(input_path)), trace_file)
            if os.path.isfile(tpath):
                events = read_trace(tpath)
        data = _run_data(manifest, events, input_path)
        title = f"detloop report: {manifest['target']['test']} seed {manifest['seed']}"
    elif lower.endswith((".jsonl", ".ndjson")):
        events = read_trace(input_path)
        data = _run_data(None, events, None)
        seed = events[0].get("seed") if events else "?"
        title = f"detloop trace report: seed {seed}"
    else:
        raise ConfigError(
            "report input must be a manifest (.json), trace (.jsonl) or results database (.sqlite)"
        )
    atomic_write(output_path, build_html(data, title))
    return output_path
