"""HTML report safety and content."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from pathlib import Path

import detloop
from detloop.report import build_html, embed_json, render_report

EVIL = "</script><script>alert(1)</script><img src=x onerror=alert(2)>  &amp;<!--"


def test_embed_json_is_inert_and_round_trips() -> None:
    text = embed_json({"x": EVIL})
    assert "<" not in text and ">" not in text and "&" not in text
    assert " " not in text and " " not in text
    assert json.loads(text) == {"x": EVIL}


def _csp_and_inline(html: str) -> tuple[str, list[str], list[str]]:
    csp = re.search(r'http-equiv="Content-Security-Policy" content="([^"]+)"', html)
    assert csp
    scripts = re.findall(r"<script>(.*?)</script>", html, re.S)
    styles = re.findall(r"<style>(.*?)</style>", html, re.S)
    return csp.group(1).replace("&#x27;", "'"), scripts, styles


def test_adversarial_trace_strings_cannot_execute(tmp_path: Path) -> None:
    async def body(world: detloop.World) -> None:
        world.note(EVIL, payload=EVIL)
        world.history.invoke(EVIL, EVIL, key=EVIL, value=EVIL).info(EVIL)
        world.check(False, EVIL, EVIL)

    res = detloop.run(body, seed=0, config=detloop.SimConfig(), keep_trace=True)
    trace = tmp_path / "t.jsonl"
    trace.write_text("".join(line + "\n" for line in res.trace_lines or []))
    out = tmp_path / "r.html"
    render_report(str(trace), str(out))
    html = out.read_text()
    outside = re.sub(r'(<script type="application/json" id="detloop-data">).*?(</script>)', r"\1\2", html, flags=re.S)
    assert "<script>alert" not in html
    assert "alert(" not in outside and "onerror" not in outside
    csp, scripts, styles = _csp_and_inline(html)
    assert len(scripts) == 1 and len(styles) == 1
    for body_text, kind in ((scripts[0], "script-src"), (styles[0], "style-src")):
        digest = base64.b64encode(hashlib.sha256(body_text.encode()).digest()).decode()
        assert f"{kind} 'sha256-{digest}'" in csp
    assert "default-src 'none'" in csp
    # the data element contains the payload only in escaped form
    data = re.search(r'<script type="application/json" id="detloop-data">(.*?)</script>', html, re.S)
    assert data and EVIL in json.loads(data.group(1))["events"][1]["note"]


def test_report_has_no_external_resources() -> None:
    html = build_html({"kind": "run", "events": [], "manifest": {}}, "t")
    assert not re.search(r"(src|href)=\"?(https?:)?//", html)
    assert "http://" not in html.replace("http://www.w3.org/2000/svg", "") and "https://" not in html
    assert "innerHTML" not in html and "eval(" not in html


def test_report_from_manifest_contains_replay_and_timeline(tmp_path: Path) -> None:
    from detloop.examples import kv_sim
    from detloop.explore import explore
    from detloop.targets import Target

    spec = detloop.get_spec(kv_sim.test_kv_stale_read)
    assert spec is not None
    r = explore(spec, [1], target=Target("detloop.examples.kv_sim", "test_kv_stale_read"), artifacts_dir=str(tmp_path))
    out = tmp_path / "r.html"
    render_report(r.artifacts[1], str(out))
    html = out.read_text()
    assert "detloop replay" in html and "test_kv_stale_read" in html
    assert len(html) < 16 * 1024 * 1024
