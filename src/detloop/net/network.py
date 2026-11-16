"""The simulated network: addressing, directed links, connections and messages."""

from __future__ import annotations

import asyncio
import contextvars
import errno
import ipaddress
import socket as _socket
from contextvars import Context
from typing import TYPE_CHECKING, Any

from .._owner import OWNER
from ..errors import UnsupportedOperation
from .messages import MessageEndpoint
from .server import SimServer
from .socket import SimSocket
from .stream import Connection, Direction, Endpoint, Segment, SimTransport, reset_error

if TYPE_CHECKING:
    from ..world import Incarnation, Node, World

NS = 1_000_000_000
US = 1_000
LOOPBACK = "127.0.0.1"
_LOOPBACK_LATENCY = 10 * US
_SERVICES = {"http": 80, "https": 443}


def _gaierror(code: str, msg: str) -> _socket.gaierror:
    return _socket.gaierror(getattr(_socket, code, -2), msg)


class Network:
    """Owns every simulated host, link, listener, connection and message port."""

    def __init__(self, world: World) -> None:
        self.world = world
        self.loop = world.loop
        self.config = world.config.network
        self.decisions = world.decisions
        self.trace = world.trace
        self.hosts: dict[str, Node] = {}
        self.names: dict[str, str] = {}
        self._blocked: dict[tuple[str, str], int] = {}
        self._factor: dict[tuple[str, str], list[float]] = {}
        self._free_at: dict[tuple[str, str], int] = {}
        self.listeners: dict[tuple[str, str, int], SimServer] = {}
        self.msg_ports: dict[tuple[str, int], MessageEndpoint] = {}
        self.connections: dict[str, Connection] = {}
        self._cid = 0
        self._msg_id = 0
        self._eph_client: dict[str, int] = {}
        self._eph_listen: dict[str, int] = {}
        self._lat_lo = round(self.config.latency_min * 1_000_000)
        self._lat_hi = round(self.config.latency_max * 1_000_000)
        self._rto = round(self.config.rto * NS)
        self._retx_timeout = round(self.config.retransmit_timeout * NS)
        self.msg_faults = "messages" in world.config.faults.kinds
        self._jitter_us = round(self.config.msg_reorder_jitter * 1_000_000)
        self.stats = {"segments": 0, "bytes": 0, "retransmits": 0, "resets": 0, "messages": 0}

    # ------------------------------------------------------------------
    # Hosts and naming
    def register_host(self, node: Node) -> None:
        self.hosts[node.address] = node
        self.names[node.name] = node.address

    def current_owner(self) -> Incarnation | None:
        return OWNER.get()  # type: ignore[no-any-return]

    def node_of(self, owner: Incarnation | None) -> Node:
        return self.world.control if owner is None else owner.node

    def current_node(self) -> Node:
        return self.node_of(self.current_owner())

    def resolve(self, host: Any) -> str:
        if isinstance(host, bytes):
            host = host.decode("idna")
        host = str(host)
        if host in ("localhost", LOOPBACK, "::1", "ip6-localhost"):
            return LOOPBACK
        if host in self.names:
            return self.names[host]
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            raise _gaierror(
                "EAI_NONAME", f"[simulated DNS] unknown host {host!r}; known: {sorted(self.names)}"
            ) from None
        if ip.version != 4:
            raise _gaierror("EAI_FAMILY", f"[simulated DNS] IPv6 address {host!r} is not simulated")
        return str(ip)

    def node_by_address(self, ip: str) -> Node | None:
        if ip == LOOPBACK:
            return self.current_node()
        return self.hosts.get(ip)

    def node_by_ref(self, ref: Any) -> Node | None:
        name = getattr(ref, "name", ref)
        if name in self.names:
            return self.hosts[self.names[name]]
        if name == "control":
            return self.world.control
        return self.hosts.get(str(name))

    def getaddrinfo(
        self,
        host: Any,
        port: Any,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[Any]:
        if port is None:
            p = 0
        elif isinstance(port, int):
            p = port
        else:
            sport = port.decode() if isinstance(port, bytes) else str(port)
            if sport.isdigit():
                p = int(sport)
            elif sport in _SERVICES:
                p = _SERVICES[sport]
            else:
                raise _gaierror("EAI_SERVICE", f"[simulated DNS] unknown service {sport!r}")
        if family not in (0, _socket.AF_INET, _socket.AF_UNSPEC):
            raise _gaierror("EAI_FAMILY", "[simulated DNS] only IPv4 (AF_INET) is simulated")
        if type not in (0, _socket.SOCK_STREAM):
            raise _gaierror("EAI_SOCKTYPE", "[simulated DNS] only SOCK_STREAM is simulated")
        if host is None or host == "":
            ip = LOOPBACK
        else:
            ip = self.resolve(host)
        return [(_socket.AF_INET, _socket.SOCK_STREAM, _socket.IPPROTO_TCP, "", (ip, p))]

    def getnameinfo(self, sockaddr: Any, flags: int = 0) -> tuple[str, str]:
        ip, port = sockaddr[0], sockaddr[1]
        node = self.hosts.get(ip)
        name = node.name if node is not None else ip
        return (name, str(port))

    # ------------------------------------------------------------------
    # Links
    def link_up(self, a: Node, b: Node) -> bool:
        return a is b or (a.name, b.name) not in self._blocked

    def block(self, a: Node, b: Node) -> None:
        key = (a.name, b.name)
        self._blocked[key] = self._blocked.get(key, 0) + 1

    def unblock(self, a: Node, b: Node) -> bool:
        key = (a.name, b.name)
        n = self._blocked.get(key, 0)
        if n <= 1:
            self._blocked.pop(key, None)
            return n == 1
        self._blocked[key] = n - 1
        return True

    def heal_all(self) -> None:
        self._blocked.clear()

    def blocked_links(self) -> list[tuple[str, str]]:
        return sorted(self._blocked)

    def push_latency_factor(self, a: Node, b: Node, factor: float) -> None:
        self._factor.setdefault((a.name, b.name), []).append(factor)

    def pop_latency_factor(self, a: Node, b: Node, factor: float) -> None:
        lst = self._factor.get((a.name, b.name))
        if lst and factor in lst:
            lst.remove(factor)
            if not lst:
                del self._factor[(a.name, b.name)]

    def latency_ns(self, a: Node, b: Node) -> int:
        if a is b:
            return _LOOPBACK_LATENCY
        us = self._lat_lo + self.decisions.choose("net", self._lat_hi - self._lat_lo + 1, "latency")
        factors = self._factor.get((a.name, b.name))
        if factors:
            f = 1.0
            for x in factors:
                f *= x
            us = round(us * f)
        return us * US

    def arrival(self, a: Node, b: Node, nbytes: int, direction: Direction | None = None) -> int:
        now = self.loop.now_ns
        start = now
        bw = self.config.bandwidth
        if bw and a is not b and nbytes:
            key = (a.name, b.name)
            start = max(now, self._free_at.get(key, 0))
            start += -(-nbytes * NS // round(bw))
            self._free_at[key] = start
        t = start + self.latency_ns(a, b)
        if direction is not None:
            t = max(t, direction.last_arrival)
            direction.last_arrival = t
        return t

    def _at(self, when: int, ctx: Context, fn: Any, *args: Any) -> asyncio.TimerHandle:
        return self.loop._call_at_ns(when, fn, args, ctx)

    def _ctx_for(self, inc: Incarnation | None) -> Context:
        return inc.context.copy() if inc is not None else self.world.control_context.copy()

    # ------------------------------------------------------------------
    # Tracing helpers
    def trace_conn(self, kind: str, ep: Endpoint, level: int = 1, **extra: Any) -> None:
        if self.trace.level >= level:
            peer = ep.peer.node.name if ep.peer is not None else None
            self.trace.emit(
                kind,
                level,
                conn=ep.conn.cid,
                side=ep.side,
                node=self._incname(ep),
                peer=peer,
                **extra,
            )

    @staticmethod
    def _incname(ep: Endpoint) -> str:
        return ep.node.name if ep.inc is None else ep.inc.label

    # ------------------------------------------------------------------
    # Servers
    async def create_server(
        self,
        protocol_factory: Any,
        host: Any = None,
        port: Any = None,
        *,
        family: int = 0,
        flags: int = 0,
        sock: Any = None,
        backlog: int = 100,
        ssl: Any = None,
        reuse_address: Any = None,
        reuse_port: Any = None,
        keep_alive: Any = None,
        ssl_handshake_timeout: Any = None,
        ssl_shutdown_timeout: Any = None,
        start_serving: bool = True,
    ) -> SimServer:
        if ssl is not None:
            raise UnsupportedOperation("TLS servers are not simulated (ssl=...)")
        if sock is not None:
            raise UnsupportedOperation(
                "create_server(sock=...) is not simulated; pass host and port"
            )
        inc = self.current_owner()
        node = self.node_of(inc)
        if host in (None, "", "0.0.0.0", "::"):  # noqa: S104 - simulated wildcard bind
            ips = [node.address, LOOPBACK]
        else:
            hosts = [host] if isinstance(host, str | bytes) else list(host)
            ips = []
            for h in hosts:
                ip = self.resolve(h)
                if ip not in (node.address, LOOPBACK):
                    raise OSError(
                        errno.EADDRNOTAVAIL,
                        f"Cannot assign requested address {h!r} on simulated node "
                        f"{node.name} ({node.address})",
                    )
                if ip not in ips:
                    ips.append(ip)
        p = int(port or 0)
        if p == 0:
            p = self._eph_listen.get(node.name, 32768)
            while any((node.name, ip, p) in self.listeners for ip in ips):
                p += 1
            self._eph_listen[node.name] = p + 1
        for ip in ips:
            if (node.name, ip, p) in self.listeners:
                raise OSError(
                    errno.EADDRINUSE, f"address already in use (simulated {node.name} {ip}:{p})"
                )
        server = SimServer(
            self, node, inc, protocol_factory, contextvars.copy_context(), [(ip, p) for ip in ips]
        )
        for ip in ips:
            self.listeners[(node.name, ip, p)] = server
        self.world._registry(inc).listeners.append(server)
        self.trace.emit("listen", 1, node=node.name if inc is None else inc.label, port=p)
        if start_serving:
            server._start_serving()
        await asyncio.sleep(0)
        return server

    def unlisten(self, server: SimServer) -> None:
        for ip, p in server._addresses:
            key = (server._node.name, ip, p)
            if self.listeners.get(key) is server:
                del self.listeners[key]
        if server._addresses:
            self.trace.emit(
                "unlisten",
                1,
                node=server._node.name if server._inc is None else server._inc.label,
                port=server._addresses[0][1],
            )

    # ------------------------------------------------------------------
    # Client connections
    async def create_connection(
        self,
        protocol_factory: Any,
        host: Any = None,
        port: Any = None,
        *,
        ssl: Any = None,
        family: int = 0,
        proto: int = 0,
        flags: int = 0,
        sock: Any = None,
        local_addr: Any = None,
        server_hostname: Any = None,
        ssl_handshake_timeout: Any = None,
        ssl_shutdown_timeout: Any = None,
        happy_eyeballs_delay: Any = None,
        interleave: Any = None,
        all_errors: bool = False,
    ) -> tuple[SimTransport, Any]:
        if ssl:
            raise UnsupportedOperation("TLS client connections are not simulated (ssl=...)")
        if server_hostname is not None:
            raise ValueError("server_hostname is only meaningful with ssl")
        if sock is not None:
            if host is not None or port is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            if not isinstance(sock, SimSocket):
                raise UnsupportedOperation(
                    "create_connection(sock=...) requires a simulated socket"
                )
            if sock._ep is None:
                raise OSError(
                    errno.ENOTCONN, "simulated socket is not connected (use loop.sock_connect)"
                )
            sock._attached = True
            ep = sock._ep
        else:
            if host is None and port is None:
                raise ValueError("host and port was not specified and no sock specified")
            infos = self.getaddrinfo(host, port, family=family, type=_socket.SOCK_STREAM)
            ip, p = infos[0][4]
            ep = await self.connect(ip, p, local_addr)
        protocol = protocol_factory()
        transport = SimTransport(ep, protocol)
        waiter: asyncio.Future[None] = self.loop.create_future()
        self.loop.call_soon(self.connection_made, ep, waiter, context=ep.ctx)
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        return transport, protocol

    async def sock_connect(self, sock: Any, address: Any) -> None:
        if not isinstance(sock, SimSocket):
            raise UnsupportedOperation("loop.sock_connect() on a real socket inside a simulation")
        if sock._ep is not None:
            raise OSError(errno.EISCONN, "simulated socket is already connected")
        ip = self.resolve(address[0])
        sock._ep = await self.connect(ip, int(address[1]), sock._bound)

    def connection_made(self, ep: Endpoint, waiter: asyncio.Future[None] | None) -> None:
        """Runs in the endpoint's context once its transport exists."""
        t = ep.transport
        assert t is not None
        t._call_protocol("connection_made", t)
        ep.connected = True
        if waiter is not None and not waiter.done():
            waiter.set_result(None)
        if ep.state == "dead":
            t._schedule_lost(getattr(ep, "pending_exc", None))
        elif ep.rbuf or ep.eof_pending:
            self.schedule_flush(ep)

    async def connect(self, ip: str, port: int, local_addr: Any = None) -> Endpoint:
        inc = self.current_owner()
        src = self.node_of(inc)
        loop = self.loop
        dst = self.node_by_address(ip)
        loopback = ip == LOOPBACK or dst is src
        src_ip = LOOPBACK if loopback else src.address
        if local_addr is not None and local_addr[1]:
            lport = int(local_addr[1])
        else:
            lport = self._eph_client.get(src.name, 49152)
            self._eph_client[src.name] = 49152 if lport >= 65535 else lport + 1
        self._cid += 1
        conn = Connection(self, f"c{self._cid}")
        client = Endpoint(
            conn, "client", src, inc, contextvars.copy_context(), (src_ip, lport), (ip, port)
        )
        conn.client = client
        fut: asyncio.Future[bool] = loop.create_future()
        conn.connect_waiter = fut
        self.connections[conn.cid] = conn
        self.world._registry(inc).endpoints[client.name] = client
        self.trace.emit(
            "connect",
            1,
            conn=conn.cid,
            node=self._incname(client),
            dst=dst.name if dst is not None else ip,
            port=port,
        )
        deadline = loop.now_ns + round(self.config.connect_timeout * NS)
        interval = NS
        try:
            while True:
                self._send_syn(conn, dst)
                remaining = deadline - loop.now_ns
                if remaining <= 0:
                    break
                await asyncio.wait([fut], timeout=min(interval, remaining) / NS)
                if fut.done():
                    break
                interval *= 2
        except BaseException:
            client.abandoned = True
            client.state = "dead"
            raise
        if not fut.done():
            client.abandoned = True
            client.state = "dead"
            self.trace.emit("connect_timeout", 1, conn=conn.cid, node=self._incname(client))
            raise TimeoutError(
                errno.ETIMEDOUT, f"connect to {ip}:{port} timed out (simulated: no response)"
            )
        exc = fut.exception()
        if exc is not None:
            client.state = "dead"
            raise exc
        self.trace.emit("established", 1, conn=conn.cid, node=self._incname(client))
        return client

    def _send_syn(self, conn: Connection, dst: Node | None) -> None:
        client = conn.client
        assert client is not None
        if dst is None or not (dst.up or self._os_answers(dst)):
            self.trace.emit("syn_lost", 2, conn=conn.cid)
            return
        dst_inc = dst.incarnation if dst.up else None
        t = self.loop.now_ns + self.latency_ns(client.node, dst)
        self._at(t, self._ctx_for(dst_inc), self._syn_arrive, conn, dst, dst_inc)

    def _syn_arrive(self, conn: Connection, dst: Node, dst_inc: Incarnation | None) -> None:
        client = conn.client
        assert client is not None
        if not self.link_up(client.node, dst):
            self.trace.emit("syn_lost", 2, conn=conn.cid)
            return
        if client.abandoned or client.state != "open":
            return
        if conn.server is not None:
            self._send_synack(conn)
            return
        ip, port = client.remote
        target_ip = LOOPBACK if ip == LOOPBACK or dst is client.node else dst.address
        server = self.listeners.get((dst.name, target_ip, port))
        if server is None or not server._serving or server._inc is not dst_inc:
            self.trace.emit("refused", 1, conn=conn.cid, node=dst.name, port=port)
            self._send_ctrl(dst, client, "refuse")
            return
        sep = Endpoint(
            conn, "server", dst, server._inc, server._ctx.copy(), (target_ip, port), client.local
        )
        conn.server = sep
        sep.peer = client
        client.peer = sep
        client.out = Direction(client, sep)
        sep.out = Direction(sep, client)
        self.world._registry(server._inc).endpoints[sep.name] = sep
        self.trace_conn("accept", sep, port=port)
        server._accept(sep)
        self._send_synack(conn)

    def _send_synack(self, conn: Connection) -> None:
        client, sep = conn.client, conn.server
        assert client is not None
        assert sep is not None
        t = self.loop.now_ns + self.latency_ns(sep.node, client.node)
        self._at(t, client.ctx, self._synack_arrive, conn)

    def _synack_arrive(self, conn: Connection) -> None:
        client, sep = conn.client, conn.server
        assert client is not None
        assert sep is not None
        if not self.link_up(sep.node, client.node):
            return
        if client.abandoned or client.state != "open":
            self._send_rst(client.node, sep)
            return
        fut = conn.connect_waiter
        if fut is not None and not fut.done():
            fut.set_result(True)

    def _send_ctrl(self, from_node: Node, target: Endpoint, kind: str) -> None:
        t = self.loop.now_ns + self.latency_ns(from_node, target.node)
        self._at(t, target.ctx, self._ctrl_arrive, target, kind, from_node)

    def _send_rst(self, from_node: Node, target: Endpoint) -> None:
        self.stats["resets"] += 1
        self._send_ctrl(from_node, target, "rst")

    def _ctrl_arrive(self, target: Endpoint, kind: str, from_node: Node) -> None:
        if not self.link_up(from_node, target.node):
            return
        fut = target.conn.connect_waiter if target.side == "client" else None
        if fut is not None and not fut.done():
            ip, port = target.remote
            fut.set_exception(
                ConnectionRefusedError(
                    errno.ECONNREFUSED,
                    f"Connect call failed ({ip!r}, {port}) (simulated: connection refused)",
                )
            )
            return
        if kind == "rst" and target.state != "dead":
            self.trace_conn("rst", target)
            self.abort_endpoint(target, None, send_rst=False, local_exc=reset_error(target))

    # ------------------------------------------------------------------
    # Data transfer
    def transmit(self, d: Direction) -> None:
        if d.stalled_since is not None or d.src.state == "dead":
            return
        dst = d.dst
        window = self.config.recv_window
        seg_size = self.config.segment_size
        sent = False
        while True:
            if d.queue:
                room = window - d.inflight_bytes - dst.rbuf_bytes
                if room <= 0:
                    break
                n = min(len(d.queue), seg_size, room)
                seg = Segment(bytes(d.queue[:n]))
                del d.queue[:n]
            elif d.fin_queued and not d.fin_sent:
                seg = Segment(b"", fin=True)
                d.fin_sent = True
            else:
                break
            d.inflight.append(seg)
            d.inflight_bytes += len(seg.data)
            self._launch(d, seg)
            sent = True
        if sent and d.src.transport is not None:
            d.src.transport._maybe_finish_close()

    def _launch(self, d: Direction, seg: Segment) -> None:
        t = self.arrival(d.src.node, d.dst.node, len(seg.data), d)
        seg.arrival = t
        self.stats["segments"] += 1
        self.stats["bytes"] += len(seg.data)
        self._at(t, d.dst.ctx, self._seg_arrive, d, seg, d.epoch)
        if d.rto_handle is None:
            d.rto_handle = self._at(t + self._rto, d.src.ctx, self._rto_fire, d)
        if self.trace.level >= 2:
            self.trace_conn("send", d.src, 2, bytes=len(seg.data), fin=seg.fin or None, at=t)

    def _seg_arrive(self, d: Direction, seg: Segment, epoch: int) -> None:
        if epoch != d.epoch or d.stalled_since is not None:
            return
        dst = d.dst
        now = self.loop.now_ns
        if not self.link_up(d.src.node, dst.node):
            d.stalled_since = now
            self.trace_conn("stall", d.src, 1, reason="partition")
            return
        if dst.state != "open":
            if seg.fin and dst.state == "closed":
                if d.inflight and d.inflight[0] is seg:
                    d.inflight.popleft()
                return
            self.trace_conn("rst_closed", dst, 2)
            self._send_rst(dst.node, d.src)
            d.reset()
            return
        if not d.inflight or d.inflight[0] is not seg:  # pragma: no cover - invariant guard
            from ..errors import InternalError

            raise InternalError(f"out-of-order segment on {d.src.name}")
        d.inflight.popleft()
        d.inflight_bytes -= len(seg.data)
        self._deliver(dst, seg)
        src_t = d.src.transport
        if d.src.state != "dead" and src_t is not None:
            src_t._maybe_resume()
        self.transmit(d)
        if not d.inflight and d.rto_handle is not None:
            d.rto_handle.cancel()
            d.rto_handle = None

    def _can_deliver(self, ep: Endpoint) -> bool:
        t = ep.transport
        return ep.connected and t is not None and not t._reading_paused and not t._lost_called

    def _deliver(self, ep: Endpoint, seg: Segment) -> None:
        if self.trace.level >= 2:
            self.trace_conn("deliver", ep, 2, bytes=len(seg.data), fin=seg.fin or None)
        if seg.fin:
            if ep.rbuf or not self._can_deliver(ep):
                ep.eof_pending = True
            else:
                self._deliver_eof(ep)
            return
        if not ep.rbuf and self._can_deliver(ep):
            assert ep.transport is not None
            ep.transport._call_protocol("data_received", seg.data)
        else:
            ep.rbuf.append(seg.data)
            ep.rbuf_bytes += len(seg.data)

    def _deliver_eof(self, ep: Endpoint) -> None:
        ep.got_eof = True
        t = ep.transport
        assert t is not None
        self.trace_conn("eof", ep)
        keep_open = t._call_protocol("eof_received")
        if not keep_open:
            t.close()

    def schedule_flush(self, ep: Endpoint) -> None:
        if not ep.flush_scheduled:
            ep.flush_scheduled = True
            self.loop.call_soon(self._flush, ep, context=ep.ctx)

    def _flush(self, ep: Endpoint) -> None:
        ep.flush_scheduled = False
        t = ep.transport
        if t is None or ep.state == "dead":
            return
        while ep.rbuf and self._can_deliver(ep):
            data = ep.rbuf.popleft()
            ep.rbuf_bytes -= len(data)
            t._call_protocol("data_received", data)
        if not ep.rbuf and ep.eof_pending and self._can_deliver(ep):
            ep.eof_pending = False
            self._deliver_eof(ep)
        if ep.peer is not None and ep.peer.out is not None:
            self.transmit(ep.peer.out)

    def _os_answers(self, node: Node) -> bool:
        """Does a node without a running process still answer with resets?

        A never-started node is a booted host with nothing listening. A crashed
        node answers in ``process`` crash mode (the OS survived) and is silent in
        ``host`` mode (the machine is gone) until it restarts."""
        if node.state == "new":
            return True
        return node.state == "down" and self.config.crash_mode == "process"

    def _host_answers(self, ep: Endpoint) -> bool:
        """Would ``ep``'s host send a reset for an unknown connection?"""
        if ep.inc is None or ep.inc.alive:
            return True
        return self.config.crash_mode == "process" or ep.node.up

    def _rto_fire(self, d: Direction) -> None:
        d.rto_handle = None
        if not d.inflight or d.src.state == "dead":
            return
        now = self.loop.now_ns
        head = d.inflight[0]
        if d.stalled_since is None:
            if head.arrival + self._rto > now:
                d.rto_handle = self._at(head.arrival + self._rto, d.src.ctx, self._rto_fire, d)
                return
            d.stalled_since = head.arrival
            self.trace_conn("stall", d.src, 1, reason="unacknowledged")
        if now - d.stalled_since >= self._retx_timeout:
            exc = TimeoutError(
                errno.ETIMEDOUT, "Connection timed out (simulated: retransmission timeout)"
            )
            self.trace_conn("timeout", d.src)
            self.abort_endpoint(d.src, None, send_rst=False, local_exc=exc)
            return
        dst = d.dst
        if self.link_up(d.src.node, dst.node):
            if dst.alive:
                d.epoch += 1
                d.stalled_since = None
                self.stats["retransmits"] += 1
                self.trace_conn("retransmit", d.src, 1, segments=len(d.inflight))
                for seg in d.inflight:
                    self._launch(d, seg)
                return
            if self._host_answers(dst) and self.link_up(dst.node, d.src.node):
                self._send_rst(dst.node, d.src)
        d.rto_handle = self._at(now + self._rto, d.src.ctx, self._rto_fire, d)

    # ------------------------------------------------------------------
    # Failure transitions
    def abort_endpoint(
        self,
        ep: Endpoint,
        _unused: Any,
        *,
        send_rst: bool,
        local_exc: BaseException | None,
    ) -> None:
        if ep.state == "dead":
            return
        ep.state = "dead"
        if ep.out is not None:
            ep.out.reset()
        ep.rbuf.clear()
        ep.rbuf_bytes = 0
        ep.eof_pending = False
        self.trace_conn("abort", ep, error=type(local_exc).__name__ if local_exc else None)
        if send_rst and ep.peer is not None and ep.peer.state != "dead":
            self._send_rst(ep.node, ep.peer)
        t = ep.transport
        if t is not None:
            if ep.alive:
                t._schedule_lost(local_exc)
        else:
            ep.pending_exc = local_exc  # type: ignore[attr-defined]
        self._maybe_forget(ep.conn)

    def _maybe_forget(self, conn: Connection) -> None:
        if all(e is None or e.state == "dead" for e in (conn.client, conn.server)):
            self.connections.pop(conn.cid, None)

    def close_all(self) -> int:
        """Close every endpoint at world teardown; returns how many were still open."""
        still_open = 0
        for conn in list(self.connections.values()):
            for ep in (conn.client, conn.server):
                if ep is None:
                    continue
                t = ep.transport
                if ep.state == "open" and ep.alive and t is not None and not t._closing:
                    still_open += 1
                ep.state = "dead"
                if ep.out is not None:
                    ep.out.reset()
                if t is not None:
                    t._closing = True
                    t._lost_called = True
        self.connections.clear()
        return still_open

    def crash_incarnation(self, inc: Incarnation) -> None:
        reg = inc.registry
        for ep in list(reg.endpoints.values()):
            if ep.state == "dead":
                continue
            ep.state = "dead"
            if ep.out is not None:
                ep.out.reset()
            ep.rbuf.clear()
            ep.rbuf_bytes = 0
            if (
                self.config.crash_mode == "process"
                and ep.peer is not None
                and ep.peer.state != "dead"
            ):
                self._send_rst(ep.node, ep.peer)
            self._maybe_forget(ep.conn)
        for server in reg.listeners:
            server._closed = True
            server._serving = False
            self.unlisten(server)
        for mep in reg.msg_endpoints:
            mep.closed = True
            self.unbind_messages(mep)

    def reset_between(self, a: Node, b: Node) -> int:
        """Reset every live connection between nodes ``a`` and ``b``."""
        count = 0
        for conn in list(self.connections.values()):
            eps = [e for e in (conn.client, conn.server) if e is not None]
            nodes = {e.node.name for e in eps}
            if len(eps) == 2 and nodes == {a.name, b.name} and any(e.state != "dead" for e in eps):
                for e in eps:
                    self.abort_endpoint(e, None, send_rst=False, local_exc=reset_error(e))
                count += 1
        return count

    # ------------------------------------------------------------------
    # Messages
    def bind_messages(self, node: Node, inc: Incarnation | None, port: int) -> MessageEndpoint:
        key = (node.name, port)
        if key in self.msg_ports:
            raise OSError(errno.EADDRINUSE, f"message port {port} already bound on {node.name}")
        ep = MessageEndpoint(self, node, inc, port)
        self.msg_ports[key] = ep
        self.world._registry(inc).msg_endpoints.append(ep)
        return ep

    def unbind_messages(self, ep: MessageEndpoint) -> None:
        key = (ep.node.name, ep.port)
        if self.msg_ports.get(key) is ep:
            del self.msg_ports[key]

    def message_send(
        self, src_ep: MessageEndpoint, dst_ref: Any, port: int, payload: bytes
    ) -> None:
        src = src_ep.node
        dst = self.node_by_ref(dst_ref)
        self._msg_id += 1
        mid = self._msg_id
        self.stats["messages"] += 1
        sender = src.name if src_ep._inc is None else src_ep._inc.label
        self.trace.emit(
            "msg_send",
            1,
            msg=mid,
            node=sender,
            dst=getattr(dst, "name", str(dst_ref)),
            port=port,
            bytes=len(payload),
        )
        if dst is None:
            self.trace.emit("msg_drop", 1, msg=mid, reason="unknown host")
            return
        copies = 1
        if self.msg_faults:
            if self.decisions.chance("net", self.config.msg_loss, "msg_loss"):
                self.trace.emit("msg_drop", 1, msg=mid, reason="loss")
                return
            if self.decisions.chance("net", self.config.msg_duplicate, "msg_dup"):
                copies = 2
        for _ in range(copies):
            lat = self.latency_ns(src, dst)
            if self.msg_faults and self._jitter_us:
                lat += self.decisions.choose("net", self._jitter_us + 1, "msg_jitter") * US
            if not dst.up:
                self.trace.emit("msg_drop", 1, msg=mid, reason="node down")
                continue
            inc = dst.incarnation
            self._at(
                self.loop.now_ns + lat,
                self._ctx_for(inc),
                self._msg_arrive,
                mid,
                src,
                dst,
                port,
                payload,
            )

    def _msg_arrive(self, mid: int, src: Node, dst: Node, port: int, payload: bytes) -> None:
        if not self.link_up(src, dst):
            self.trace.emit("msg_drop", 1, msg=mid, reason="partition")
            return
        ep = self.msg_ports.get((dst.name, port))
        if ep is None or not ep._deliver(src.name, payload, self.config.msg_queue_limit):
            self.trace.emit(
                "msg_drop", 1, msg=mid, reason="no receiver" if ep is None else "queue full"
            )
            return
        self.trace.emit("msg_deliver", 1, msg=mid, node=dst.name)
