"""Simulated socket objects.

Inside a simulation ``socket.socket(AF_INET, SOCK_STREAM)`` returns a
:class:`SimSocket`. It supports only what non-blocking asyncio connection
establishment needs (``setblocking``, ``setsockopt``, ``bind``,
``loop.sock_connect``, then ``loop.create_connection(sock=...)``), which is
the path used by libraries such as aiohttp (through aiohappyeyeballs). Any
attempt to do blocking or raw I/O raises an actionable error; no real
socket is ever created.
"""

from __future__ import annotations

import errno
import socket as _socket
from typing import TYPE_CHECKING, Any

from ..errors import UnsupportedOperation

if TYPE_CHECKING:
    from .stream import Endpoint

AF_INET = _socket.AF_INET
AF_INET6 = _socket.AF_INET6
SOCK_STREAM = _socket.SOCK_STREAM
IPPROTO_TCP = _socket.IPPROTO_TCP
_TYPE_FLAGS = getattr(_socket, "SOCK_NONBLOCK", 0) | getattr(_socket, "SOCK_CLOEXEC", 0)


class TransportSocketView:
    """What ``transport.get_extra_info("socket")`` returns for a simulated stream."""

    family = AF_INET
    type = SOCK_STREAM
    proto = IPPROTO_TCP

    def __init__(self, ep: Endpoint) -> None:
        self._ep = ep

    def fileno(self) -> int:
        return -1

    def getsockname(self) -> tuple[str, int]:
        return self._ep.local

    def getpeername(self) -> tuple[str, int]:
        return self._ep.remote

    def setsockopt(self, *args: Any) -> None:
        return None

    def getsockopt(self, level: int, optname: int, buflen: int | None = None) -> int:
        return 0

    def setblocking(self, flag: bool) -> None:
        return None

    def gettimeout(self) -> float:
        return 0.0

    def __repr__(self) -> str:
        return f"<simulated socket {self._ep.local} -> {self._ep.remote}>"


class SimSocket:
    """Stand-in for ``socket.socket`` inside a simulation (TCP/IPv4 only)."""

    def __init__(
        self,
        family: int = AF_INET,
        type: int = SOCK_STREAM,
        proto: int = 0,
        fileno: int | None = None,
    ) -> None:
        from ..guards import violation

        if fileno is not None:
            raise violation("socket.socket(fileno=...) wraps a real file descriptor")
        base_type = type & ~_TYPE_FLAGS
        if family not in (AF_INET, AF_INET6) or base_type != SOCK_STREAM:
            raise violation(
                f"socket.socket(family={family}, type={type}) is not simulated; only TCP "
                "stream sockets are available inside a simulation (use asyncio streams)"
            )
        self.family = family
        self.type = SOCK_STREAM
        self.proto = proto or IPPROTO_TCP
        self._blocking = True
        self._timeout: float | None = None
        self._bound: tuple[str, int] | None = None
        self._opts: dict[tuple[int, int], Any] = {}
        self._ep: Endpoint | None = None
        self._attached = False
        self._closed = False

    # option handling -------------------------------------------------------
    def setblocking(self, flag: bool) -> None:
        self._blocking = bool(flag)

    def settimeout(self, value: float | None) -> None:
        self._timeout = value
        self._blocking = value is None or value > 0

    def gettimeout(self) -> float | None:
        return self._timeout if self._blocking else 0.0

    def setsockopt(self, level: int, optname: int, value: Any, optlen: int | None = None) -> None:
        self._opts[(level, optname)] = value

    def getsockopt(self, level: int, optname: int, buflen: int | None = None) -> Any:
        return self._opts.get((level, optname), 0)

    def bind(self, address: tuple[str, int]) -> None:
        self._bound = (str(address[0]), int(address[1]))

    def fileno(self) -> int:
        return -1

    def getsockname(self) -> tuple[str, int]:
        if self._ep is not None:
            return self._ep.local
        return self._bound or ("0.0.0.0", 0)  # noqa: S104 - unbound socket address

    def getpeername(self) -> tuple[str, int]:
        if self._ep is None:
            raise OSError(errno.ENOTCONN, "Transport endpoint is not connected")
        return self._ep.remote

    # forbidden operations ------------------------------------------------
    def connect(self, address: Any) -> None:
        from ..guards import violation

        raise violation(
            "blocking socket.connect() inside a simulation; use asyncio.open_connection() "
            "or loop.sock_connect() on a non-blocking socket"
        )

    def connect_ex(self, address: Any) -> int:
        self.connect(address)
        return 0  # pragma: no cover

    def listen(self, backlog: int = 0) -> None:
        raise UnsupportedOperation(
            "listening on raw sockets; use asyncio.start_server/loop.create_server"
        )

    def accept(self) -> Any:
        raise UnsupportedOperation("socket.accept(); use asyncio.start_server/loop.create_server")

    def _no_io(self, *args: Any, **kwargs: Any) -> Any:
        raise UnsupportedOperation("raw socket I/O inside a simulation; use transports or streams")

    send = sendall = recv = recv_into = sendto = recvfrom = _no_io

    def shutdown(self, how: int) -> None:
        return None

    def detach(self) -> int:
        return -1

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._ep is not None and not self._attached:
            self._ep.conn.network.abort_endpoint(self._ep, None, send_rst=True, local_exc=None)

    def __enter__(self) -> SimSocket:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        where = f" {self._ep.local}->{self._ep.remote}" if self._ep else ""
        return f"<SimSocket{where}{' closed' if self._closed else ''}>"
