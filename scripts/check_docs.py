"""Documentation checks (run in CI and locally).

1. Every relative Markdown link points to an existing file or directory, and
   every ``#anchor`` into a Markdown file matches a heading.
2. A code block preceded by ``<!-- example: path -->`` is byte-identical
   (modulo line endings) to that file, which the test suite executes.
3. No document references an external CDN script or stylesheet.

Usage: python scripts/check_docs.py [--external]   (--external also checks http(s) links)
"""

from __future__ import annotations

import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)\)")
EXAMPLE = re.compile(r"<!-- example: (\S+) -->\s*\n```[a-z]*\n(.*?)```", re.S)


def slug(heading: str) -> str:
    s = heading.strip().lower()
    s = re.sub(r"[^\w\- ]", "", s)
    return s.replace(" ", "-")


def anchors(path: Path) -> set[str]:
    out = set()
    in_code = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("```"):
            in_code = not in_code
        if not in_code and line.startswith("#"):
            out.add(slug(line.lstrip("#")))
    return out


def main() -> int:
    check_external = "--external" in sys.argv
    errors: list[str] = []
    docs = [
        p
        for p in ROOT.rglob("*.md")
        if not any(part.startswith(".") for part in p.relative_to(ROOT).parts)
        and "node_modules" not in p.parts
        and ".venv" not in str(p)
    ]
    for doc in sorted(docs):
        text = doc.read_text(encoding="utf-8")
        stripped = re.sub(r"```.*?```", "", text, flags=re.S)
        for target in LINK.findall(stripped):
            if target.startswith(("http://", "https://")):
                if check_external:
                    try:
                        req = urllib.request.Request(
                            target, method="HEAD", headers={"User-Agent": "docs-check"}
                        )
                        urllib.request.urlopen(req, timeout=15)
                    except Exception as exc:
                        errors.append(f"{doc.relative_to(ROOT)}: external link {target}: {exc}")
                continue
            if target.startswith("mailto:"):
                continue
            path_part, _, anchor = target.partition("#")
            dest = (doc.parent / path_part).resolve() if path_part else doc
            if not dest.exists():
                errors.append(f"{doc.relative_to(ROOT)}: broken link {target}")
                continue
            if anchor and dest.suffix == ".md" and anchor not in anchors(dest):
                errors.append(f"{doc.relative_to(ROOT)}: missing anchor {target}")
        for rel, code in EXAMPLE.findall(text):
            src = ROOT / rel
            if not src.exists():
                errors.append(f"{doc.relative_to(ROOT)}: example file {rel} missing")
                continue
            want = src.read_text(encoding="utf-8").replace("\r\n", "\n")
            if code.replace("\r\n", "\n") != want:
                errors.append(f"{doc.relative_to(ROOT)}: code block differs from {rel}")
        if re.search(r"<script[^>]+src=|<link[^>]+href=[\"']https?:", text):
            errors.append(f"{doc.relative_to(ROOT)}: references external assets")
    for e in errors:
        print(e)
    print(f"checked {len(docs)} documents: {'OK' if not errors else f'{len(errors)} problem(s)'}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
