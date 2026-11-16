"""Loading simulation targets for the CLI and worker processes.

Target syntax:

* ``path/to/file.py`` - every ``@detloop.simulate`` test in the file
* ``path/to/file.py::test_name`` - one test
* ``package.module`` / ``package.module::test_name`` - importable modules

Files are imported like pytest's ``prepend`` import mode: the first ancestor
directory without ``__init__.py`` is put on ``sys.path`` and the file is
imported by its dotted package path.
"""

from __future__ import annotations

import hashlib
import importlib
import os
import sys
from dataclasses import dataclass
from types import ModuleType

from .api import Spec, get_spec
from .errors import TargetError


@dataclass(frozen=True)
class Target:
    """A resolvable reference to one simulation test."""

    location: str  # file path or module name, as given
    test: str  # qualname

    def __str__(self) -> str:
        return f"{self.location}::{self.test}"


def split_target(target: str) -> tuple[str, str | None]:
    if "::" in target:
        loc, name = target.split("::", 1)
        return loc, name or None
    return target, None


def _is_file(loc: str) -> bool:
    return loc.endswith(".py") or os.sep in loc or "/" in loc


def import_location(loc: str) -> ModuleType:
    if _is_file(loc):
        path = os.path.abspath(loc)
        if not os.path.isfile(path):
            raise TargetError(f"target file not found: {loc}")
        directory, filename = os.path.split(path)
        parts = [os.path.splitext(filename)[0]]
        while os.path.isfile(os.path.join(directory, "__init__.py")):
            directory, pkg = os.path.split(directory)
            parts.insert(0, pkg)
        modname = ".".join(parts)
        if directory not in sys.path:
            sys.path.insert(0, directory)
        existing = sys.modules.get(modname)
        if existing is not None:
            mod_file = getattr(existing, "__file__", None)
            if mod_file and os.path.abspath(mod_file) == path:
                return existing
            raise TargetError(
                f"module name {modname!r} for {loc} clashes with an already imported module"
            )
        try:
            return importlib.import_module(modname)
        except TargetError:
            raise
        except Exception as exc:
            raise TargetError(f"failed to import {loc}: {type(exc).__name__}: {exc}") from exc
    try:
        return importlib.import_module(loc)
    except Exception as exc:
        raise TargetError(f"failed to import module {loc!r}: {type(exc).__name__}: {exc}") from exc


def discover(target: str) -> list[tuple[Target, Spec]]:
    """All simulation specs selected by ``target`` (in definition order)."""
    loc, name = split_target(target)
    module = import_location(loc)
    found: list[tuple[Target, Spec]] = []
    for attr, obj in vars(module).items():
        spec = get_spec(obj)
        if spec is None or (name is not None and name not in (attr, spec.qualname)):
            continue
        found.append((Target(loc, spec.qualname), spec))
    if not found:
        what = f"test {name!r}" if name else "any @detloop.simulate test"
        raise TargetError(f"{loc} does not define {what}")
    return found


def load(target: Target) -> Spec:
    specs = discover(f"{target.location}::{target.test}")
    return specs[0][1]


def fingerprint(spec: Spec) -> str:
    """SHA-256 of the defining source file with newlines normalized."""
    module = sys.modules.get(spec.module)
    path = getattr(module, "__file__", None)
    if not path or not os.path.isfile(path):
        return ""
    with open(path, "rb") as fh:
        data = fh.read().replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()
