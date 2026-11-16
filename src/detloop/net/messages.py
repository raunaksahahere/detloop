"""Datagram-style message bus with application-visible faults.

Unlike streams, messages are *independent*: each draws its own latency (so
they can be reordered), and when the ``messages`` fault kind is enabled each
one can be lost or duplicated. Messages to a blocked link, a down node, or an
unbound port are dropped. Payloads must be ``bytes`` (copy semantics).
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..world import Incarnation, Node
    from .network import Network


class MessageEndpoint:
    """A bound message port on a node. Obtain one with :meth:`Node.messages`."""

    def __init__(self, network: Network, node: Node, inc: Incarnation | None, port: int) -> None:
        self._network = network
        self.node = node
        self._inc = inc
        self.port = port
        self._queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()
        self.closed = False
        self.dropped = 0

    def send(self, dst: str, port: int, payload: bytes) -> None:
        """Send ``payload`` to ``port`` on node ``dst`` (name or address). Never blocks."""
        if self.closed:
            raise ConnectionError(f"message endpoint {self.node.name}:{self.port} is closed")
        if not isinstance(payload, bytes | bytearray | memoryview):
            raise TypeError("message payloads must be bytes-like")
        self._network.message_send(self, dst, int(port), bytes(payload))

    async def recv(self) -> tuple[str, bytes]:
        """Wait for the next message; returns ``(source node name, payload)``."""
        return await self._queue.get()

    def recv_nowait(self) -> tuple[str, bytes]:
        return self._queue.get_nowait()

    def pending(self) -> int:
        return self._queue.qsize()

    def _deliver(self, src: str, payload: bytes, limit: int) -> bool:
        if self.closed or self._queue.qsize() >= limit:
            self.dropped += 1
            return False
        self._queue.put_nowait((src, payload))
        return True

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._network.unbind_messages(self)

    def __repr__(self) -> str:
        return f"<MessageEndpoint {self.node.name}:{self.port}{' closed' if self.closed else ''}>"
