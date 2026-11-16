"""Quoting for commands that detloop prints for users to copy."""

from __future__ import annotations

import os
import shlex
import subprocess


def quote(arg: str) -> str:
    """Quote ``arg`` for the current platform's shell (cmd/PowerShell on Windows)."""
    if os.name == "nt":
        return subprocess.list2cmdline([arg])
    return shlex.quote(arg)
