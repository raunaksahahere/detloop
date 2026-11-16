"""Simulated listening server (``asyncio.AbstractServer``)."""

from __future__ import annotations

import asyncio
from contextvars import Context
from typing import TYPE_CHECKING, Any

from .._compat import PY_VERSION

if TYPE_CHECKING:
    from ..world import Incarnation, Node
    from .network import Network
    from .stream import Endpoint, SimTransport


class ListenerSocketView:
    """Minimal socket-like view returned by :attr:`SimServer.sockets`."""

    def __init__(self, address: tuple[str, int]) -> None:
        self._address = address

    @property
    def family(self) -> int:
        import socket

        return socket.AF_INET

    @property
    def type(self) -> int:
        import socket

        return socket.SOCK_STREAM

    def getsockname(self) -> tuple[str, int]:
        return self._address

    def fileno(self) -> int:
        return -1

    def setsockopt(self, *args: Any) -> None:
        return None

    def __repr__(self) -> str:
        return f"<simulated listening socket {self._address[0]}:{self._address[1]}>"


class SimServer(asyncio.AbstractServer):
    """Server returned by ``loop.create_server`` / ``asyncio.start_server`` in a simulation.

    ``wait_closed()`` follows the semantics of the running Python version:
    on 3.12+ it waits until the server is closed *and* all its connections are
    gone; on 3.11 it returns immediately once the server is closed.
    """

    def __init__(
        self,
        network: Network,
        node: Node,
        inc: Incarnation | None,
        protocol_factory: Any,
        ctx: Context,
        addresses: list[tuple[str, int]],
    ) -> None:
        self._network = network
        self._loop = network.loop
        self._node = node
        self._inc = inc
        self._protocol_factory = protocol_factory
        self._ctx = ctx
        self._addresses = addresses
        self._clients: dict[int, SimTransport] = {}
        self._client_seq = 0
        self._waiters: list[asyncio.Future[None]] | None = []
        self._serving = False
        self._closed = False
        self._serving_forever_fut: asyncio.Future[None] | None = None

    def __repr__(self) -> str:
        return f"<SimServer {self._node.name} {self._addresses} serving={self._serving}>"

    # -- connection bookkeeping ------------------------------------------
    def _attach(self, transport: SimTransport) -> None:
        self._client_seq += 1
        transport._server_key = self._client_seq  # type: ignore[attr-defined]
        self._clients[self._client_seq] = transport

    def _detach(self, transport: SimTransport) -> None:
        self._clients.pop(getattr(transport, "_server_key", -1), None)
        if not self._clients and self._closed:
            self._wakeup()

    def _wakeup(self) -> None:
        waiters, self._waiters = self._waiters, None
        for waiter in waiters or ():
            if not waiter.done():
                waiter.set_result(None)

    def _accept(self, ep: Endpoint) -> None:
        self._loop.call_soon(self._accept_connection, ep, context=ep.ctx)

    def _accept_connection(self, ep: Endpoint) -> None:
        from .stream import SimTransport

        if self._closed and ep.state != "open":
            return
        try:
            protocol = self._protocol_factory()
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._loop.call_exception_handler(
                {"message": "Error in protocol factory of simulated server", "exception": exc}
            )
            self._network.abort_endpoint(ep, None, send_rst=True, local_exc=None)
            return
        transport = SimTransport(ep, protocol, server=self)
        self._network.connection_made(ep, None)
        del transport

    # -- AbstractServer API ------------------------------------------------
    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def is_serving(self) -> bool:
        return self._serving

    @property
    def sockets(self) -> tuple[ListenerSocketView, ...]:
        if self._closed:
            return ()
        return tuple(ListenerSocketView(a) for a in self._addresses)

    def _start_serving(self) -> None:
        if self._closed:
            return
        self._serving = True

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._serving = False
        self._network.unlisten(self)
        fut = self._serving_forever_fut
        if fut is not None and not fut.done():
            fut.cancel()
            self._serving_forever_fut = None
        if not self._clients:
            self._wakeup()

    def close_clients(self) -> None:
        for transport in list(self._clients.values()):
            transport.close()

    def abort_clients(self) -> None:
        for transport in list(self._clients.values()):
            transport.abort()

    async def start_serving(self) -> None:
        self._start_serving()
        await asyncio.sleep(0)

    async def serve_forever(self) -> None:
        if self._serving_forever_fut is not None:
            raise RuntimeError(f"server {self!r} is already being awaited on serve_forever()")
        if self._closed:
            raise RuntimeError(f"server {self!r} is closed")
        self._start_serving()
        self._serving_forever_fut = self._loop.create_future()
        try:
            await self._serving_forever_fut
        except asyncio.CancelledError:
            try:
                self.close()
                if PY_VERSION >= (3, 13):
                    self.close_clients()
                await self.wait_closed()
            finally:
                raise
        finally:
            self._serving_forever_fut = None

    async def wait_closed(self) -> None:
        if PY_VERSION < (3, 12):
            if self._closed or self._waiters is None:
                return
        if self._waiters is None:
            return
        waiter: asyncio.Future[None] = self._loop.create_future()
        self._waiters.append(waiter)
        await waiter

    async def __aenter__(self) -> SimServer:
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.close()
        await self.wait_closed()
