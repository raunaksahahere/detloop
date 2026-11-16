"""TCP-like reliable ordered streams.

Model (normative; see docs/FAULT_MODEL.md):

* A connection has two endpoints; each direction carries an ordered byte
  stream cut into segments of at most ``segment_size`` bytes.
* Each segment's arrival time is ``send time (+ serialization at the link
  bandwidth) + one-way latency``, forced to be non-decreasing per direction,
  so bytes are never reordered.
* Arrival handlers are owned by the receiving incarnation. A segment that
  arrives while the directed link is blocked, or whose handler is dropped
  because the receiver crashed, is *lost*; the direction becomes **stalled**
  and every later segment in that direction is held back too. The sender's
  retransmission timer (``rto``) resends all unacknowledged data in order
  once the link is usable. A direction stalled for longer than
  ``retransmit_timeout`` aborts the sender with ``TimeoutError``.
* Acknowledgement is implicit and instantaneous at delivery.
* Receiver window: data in flight plus data buffered while the receiver has
  paused reading may not exceed ``recv_window``; the remainder waits in the
  sender's write buffer, which drives ``pause_writing``/``resume_writing``
  and therefore ``StreamWriter.drain()``.
* ``close()`` sends FIN after buffered data; ``write_eof()`` half-closes;
  ``abort()`` discards buffers and resets the peer. Data arriving at a fully
  closed or aborted endpoint triggers a reset (RST) back to the sender.
* Control packets (SYN, SYN-ACK, RST) are subject to partitions and are
  simply lost when a link is blocked.
"""

from __future__ import annotations

import asyncio
import collections
import errno
from contextvars import Context
from typing import TYPE_CHECKING, Any

from ..errors import BudgetExceeded

if TYPE_CHECKING:
    from ..world import Incarnation, Node
    from .network import Network
    from .server import SimServer

NS = 1_000_000_000


class Segment:
    __slots__ = ("arrival", "data", "fin")

    def __init__(self, data: bytes, fin: bool = False) -> None:
        self.data = data
        self.fin = fin
        self.arrival = 0


class Direction:
    """One direction of a connection: ``src`` endpoint sends to ``dst``."""

    __slots__ = (
        "dst",
        "epoch",
        "fin_queued",
        "fin_sent",
        "inflight",
        "inflight_bytes",
        "last_arrival",
        "queue",
        "rto_handle",
        "src",
        "stalled_since",
    )

    def __init__(self, src: Endpoint, dst: Endpoint) -> None:
        self.src = src
        self.dst = dst
        self.queue = bytearray()
        self.inflight: collections.deque[Segment] = collections.deque()
        self.inflight_bytes = 0
        self.epoch = 0
        self.stalled_since: int | None = None
        self.last_arrival = 0
        self.rto_handle: asyncio.TimerHandle | None = None
        self.fin_queued = False
        self.fin_sent = False

    def buffered(self) -> int:
        return len(self.queue) + self.inflight_bytes

    def reset(self) -> None:
        self.queue.clear()
        self.inflight.clear()
        self.inflight_bytes = 0
        self.epoch += 1
        self.stalled_since = None
        if self.rto_handle is not None:
            self.rto_handle.cancel()
            self.rto_handle = None


class Connection:
    __slots__ = ("cid", "client", "connect_waiter", "network", "server", "src_port")

    def __init__(self, network: Network, cid: str) -> None:
        self.network = network
        self.cid = cid
        self.client: Endpoint | None = None
        self.server: Endpoint | None = None
        self.connect_waiter: asyncio.Future[bool] | None = None
        self.src_port = 0


class Endpoint:
    """One side of a connection. ``state``: open, closed (user closed), dead."""

    def __init__(
        self,
        conn: Connection,
        side: str,
        node: Node,
        inc: Incarnation | None,
        ctx: Context,
        local: tuple[str, int],
        remote: tuple[str, int],
    ) -> None:
        self.conn = conn
        self.side = side
        self.node = node
        self.inc = inc
        self.ctx = ctx
        self.local = local
        self.remote = remote
        self.peer: Endpoint | None = None
        self.out: Direction | None = None  # set when both endpoints exist
        self.state = "open"
        self.transport: SimTransport | None = None
        self.connected = False  # connection_made has been called
        self.rbuf: collections.deque[bytes] = collections.deque()
        self.rbuf_bytes = 0
        self.eof_pending = False
        self.got_eof = False
        self.flush_scheduled = False
        self.abandoned = False  # client gave up connecting

    @property
    def name(self) -> str:
        return f"{self.conn.cid}.{self.side}"

    @property
    def alive(self) -> bool:
        return self.inc is None or self.inc.alive


class SimTransport(asyncio.Transport):
    """Transport for a simulated stream endpoint."""

    def __init__(
        self, ep: Endpoint, protocol: asyncio.BaseProtocol, server: SimServer | None = None
    ) -> None:
        super().__init__(extra=None)
        self._ep = ep
        self._protocol: Any = protocol
        self._net = ep.conn.network
        self._loop = self._net.loop
        self._server = server
        self._closing = False
        self._lost_called = False
        self._eof_written = False
        self._reading_paused = False
        self._writing_paused = False
        self._high = 64 * 1024
        self._low = 16 * 1024
        self._dropped_writes = 0
        ep.transport = self
        if server is not None:
            server._attach(self)

    # -- BaseTransport -------------------------------------------------
    def get_extra_info(self, name: str, default: Any = None) -> Any:
        ep = self._ep
        if name == "peername":
            return ep.remote
        if name == "sockname":
            return ep.local
        if name == "socket":
            from .socket import TransportSocketView

            return TransportSocketView(ep)
        if name in ("sslcontext", "ssl_object", "peercert", "cipher", "compression"):
            return None
        return default

    def is_closing(self) -> bool:
        return self._closing or self._ep.state != "open"

    def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        ep = self._ep
        self._reading_paused = True
        if ep.state != "open":
            self._schedule_lost(None)
            return
        if ep.out is not None:
            ep.out.fin_queued = True
            self._net.transmit(ep.out)
        self._maybe_finish_close()

    def _maybe_finish_close(self) -> None:
        ep = self._ep
        if self._closing and ep.state == "open" and (ep.out is None or not ep.out.queue):
            ep.state = "closed"
            self._net.trace_conn("close", ep)
            self._schedule_lost(None)

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._protocol = protocol

    def get_protocol(self) -> asyncio.BaseProtocol:
        return self._protocol  # type: ignore[no-any-return]

    # -- ReadTransport -------------------------------------------------
    def is_reading(self) -> bool:
        return not self._reading_paused and not self.is_closing()

    def pause_reading(self) -> None:
        if self._closing or self._reading_paused:
            return
        self._reading_paused = True

    def resume_reading(self) -> None:
        if self._closing or not self._reading_paused:
            return
        self._reading_paused = False
        self._net.schedule_flush(self._ep)

    # -- WriteTransport ------------------------------------------------
    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        if high is None:
            high = 64 * 1024 if low is None else 4 * low
        if low is None:
            low = high // 4
        if not high >= low >= 0:
            raise ValueError(f"high ({high!r}) must be >= low ({low!r}) must be >= 0")
        self._high, self._low = high, low
        self._maybe_pause()

    def get_write_buffer_limits(self) -> tuple[int, int]:
        return (self._low, self._high)

    def get_write_buffer_size(self) -> int:
        out = self._ep.out
        return out.buffered() if out is not None else 0

    def write(self, data: bytes | bytearray | memoryview) -> None:
        if not isinstance(data, bytes | bytearray | memoryview):
            raise TypeError(
                f"data argument must be a bytes-like object, not {type(data).__name__!r}"
            )
        if self._eof_written:
            raise RuntimeError("Cannot call write() after write_eof()")
        if not data:
            return
        ep = self._ep
        if ep.state != "open" or self._lost_called or ep.out is None:
            self._dropped_writes += 1
            return
        out = ep.out
        out.queue += data
        limit = self._net.config.max_write_buffer
        if out.buffered() > limit:
            exc = BudgetExceeded(
                "buffer",
                f"write buffer of {ep.name} exceeded max_write_buffer={limit} bytes "
                "(the writer ignores backpressure; await writer.drain())",
            )
            self._net.loop.abort(exc)
        self._net.transmit(out)
        self._maybe_pause()

    def writelines(self, list_of_data: Any) -> None:
        self.write(b"".join(list_of_data))

    def write_eof(self) -> None:
        if self._closing or self._eof_written:
            return
        self._eof_written = True
        ep = self._ep
        if ep.state == "open" and ep.out is not None:
            ep.out.fin_queued = True
            self._net.transmit(ep.out)

    def can_write_eof(self) -> bool:
        return True

    def abort(self) -> None:
        self._net.abort_endpoint(self._ep, None, send_rst=True, local_exc=None)

    # -- flow control helpers -------------------------------------------
    def _maybe_pause(self) -> None:
        if self._writing_paused or self._lost_called:
            return
        if self.get_write_buffer_size() > self._high:
            self._writing_paused = True
            try:
                self._protocol.pause_writing()
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                self._loop.call_exception_handler(
                    {
                        "message": "protocol.pause_writing() failed",
                        "exception": exc,
                        "transport": self,
                        "protocol": self._protocol,
                    }
                )

    def _maybe_resume(self) -> None:
        if (
            self._writing_paused
            and self.get_write_buffer_size() <= self._low
            and not self._lost_called
        ):
            self._writing_paused = False
            self._loop.call_soon(self._call_protocol, "resume_writing", context=self._ep.ctx)

    def _call_protocol(self, method: str, *args: Any) -> Any:
        if self._lost_called and method != "connection_lost":
            return None
        try:
            return getattr(self._protocol, method)(*args)
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._fatal_error(exc, f"Fatal error: protocol.{method}() call failed.")
            return None

    def _fatal_error(self, exc: BaseException, message: str) -> None:
        self._loop.call_exception_handler(
            {"message": message, "exception": exc, "transport": self, "protocol": self._protocol}
        )
        self._net.abort_endpoint(self._ep, None, send_rst=True, local_exc=exc)

    def _schedule_lost(self, exc: BaseException | None) -> None:
        if self._lost_called:
            return
        self._lost_called = True
        if self._loop.is_closed():  # world already torn down
            return
        self._loop.call_soon(self._connection_lost, exc, context=self._ep.ctx)

    def _connection_lost(self, exc: BaseException | None) -> None:
        try:
            self._protocol.connection_lost(exc)
        finally:
            if self._server is not None:
                server, self._server = self._server, None
                server._detach(self)

    def __repr__(self) -> str:
        ep = self._ep
        return f"<SimTransport {ep.name} {ep.local}->{ep.remote} state={ep.state}>"


def reset_error(ep: Endpoint) -> ConnectionResetError:
    return ConnectionResetError(
        errno.ECONNRESET, f"Connection reset by peer (simulated, {ep.name})"
    )
