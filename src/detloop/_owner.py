"""Ownership tokens.

Every callback, timer, task and transport in a simulation is owned either by the
*control* context (the test body, ``None``) or by a node *incarnation*. The
owner travels in a :class:`contextvars.ContextVar`, which asyncio already
propagates into tasks (``create_task`` copies the current context), into
``call_soon``/``call_later`` handles, and into future done-callbacks (registered
with the awaiting task's context). That makes ownership follow descendants
created by service code without wrapping any asyncio API.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Any


class Owner:
    """Something that can own simulated activity. ``alive`` turns false on crash."""

    __slots__ = ("alive", "label")

    def __init__(self, label: str) -> None:
        self.alive = True
        self.label = label

    def __repr__(self) -> str:
        state = "alive" if self.alive else "dead"
        return f"<Owner {self.label} {state}>"


#: The owner of the currently executing code (``None`` = control/test body).
OWNER: ContextVar[Any] = ContextVar("detloop.owner", default=None)


def owner_label(owner: Owner | None) -> str:
    return "control" if owner is None else owner.label
