# Security policy

## Scope and threat model

detloop is test tooling. It runs the code under test **in the same Python
process**. Its determinism guards replace a documented set of module
attributes; they are **not a security sandbox**. Code that wants to escape the
simulation can (C extensions, cached aliases, `ctypes`, direct system calls).
Only run code you trust, exactly as you would with pytest.

In scope:

- Vulnerabilities in detloop itself, for example in the CLI, artifact handling
  (path traversal in artifact names, unsafe manifest parsing), or the HTML
  report (script execution from trace content, external resource loading).
- Release integrity problems (published artifacts that do not match the
  tagged source or `SHA256SUMS`).

Out of scope: code under test escaping the simulation, or denial of service
by code under test (use the wall-clock timeout).

## Reporting a vulnerability

Please report privately through GitHub's *Report a vulnerability* button
(Security → Advisories) on <https://github.com/raunaksahahere/detloop>. Do not
open a public issue. Include affected versions, a description, and a minimal
reproduction. You should receive an acknowledgement within a week.

## Supported versions

Security fixes are released for the latest minor version only.
