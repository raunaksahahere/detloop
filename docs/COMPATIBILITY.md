# Compatibility

## Runtimes

| | Status |
|---|---|
| CPython 3.11, 3.12, 3.13, 3.14 | Supported; full test suite in CI on Linux, macOS and Windows |
| CPython 3.15+ | Unsupported until validated (see [MAINTENANCE.md](MAINTENANCE.md)) |
| PyPy, GraalPy | Unsupported |
| uvloop and other loop implementations | Not applicable: detloop *is* the loop; code that requires uvloop-specific APIs is unsupported |
| Free-threaded CPython builds | Not tested |

Locally verified before release: CPython 3.11.16, 3.12.3, 3.13.15, 3.14.7 on
Linux x86_64. CI results for each OS are linked from the release notes.

## Libraries tested over simulated transports

Exercised with real traffic (no mocks) in `tests/compat/test_http_libraries.py`:

| Library | Tested version | What works | Known limitation |
|---|---|---|---|
| aiohttp (server: `web.AppRunner`/`TCPSite`) | 3.14.5 | JSON, POST, streaming responses, concurrent requests, graceful cleanup | — |
| aiohttp (client: `ClientSession`) | 3.14.5 (aiohappyeyeballs 2.7.1) | requests, keep-alive reuse, `ClientTimeout`, connection errors during partitions and crashes | — |
| httpx `AsyncClient` (httpcore 1.0.9, anyio 4.16.0) | 0.28.1 | requests, POST, streaming, `ReadTimeout`, `ConnectTimeout`, `ConnectError`, transport errors on crash | **No keep-alive reuse**: httpcore checks pooled connections by polling the socket's file descriptor; simulated sockets have none (`fileno() == -1`), which httpcore treats as "closed", so it opens a new connection per request. Requests still succeed |

The compatibility extra pins the ranges `aiohttp>=3.10,<4` and
`httpx>=0.27,<0.29`; only the versions in the table were tested.

HTTP only: TLS (`https://`, `ssl=`) is rejected, including user-space TLS over
memory BIOs (anyio), because OpenSSL draws entropy the seed does not control.

## asyncio feature support

| Feature | Status |
|---|---|
| Tasks, futures, `gather`, `wait`, `wait_for`, `shield`, `timeout`/`timeout_at`, `TaskGroup`, cancellation, `current_task`, `all_tasks` | Supported |
| `Lock`, `Event`, `Condition`, `Semaphore`, `BoundedSemaphore`, `Barrier`, `Queue` family | Supported |
| Async generators (finalization via loop hooks, `shutdown_asyncgens`) | Supported |
| `contextvars` propagation | Supported |
| Eager task factory (3.12+) | Supported |
| `call_soon`, `call_later`, `call_at`, `call_soon_threadsafe` (same thread) | Supported; cross-thread calls are a nondeterminism violation |
| Streams (`open_connection`, `start_server`, `StreamReader`/`StreamWriter`, `drain`, `write_eof`, `wait_closed`) | Supported (TCP, IPv4 only) |
| `create_connection`, `create_server` (`host`/`port`, `sock=` from a simulated socket, `start_serving`, `reuse_*`, `backlog` accepted) | Supported |
| `sock_connect` with a simulated socket | Supported (used by aiohttp's happy eyeballs) |
| `getaddrinfo`, `getnameinfo` | Simulated DNS (node names, IPv4 literals, `localhost`) |
| `run_in_executor`, `asyncio.to_thread` | Rejected (`NondeterminismError`) unless `executor="inline"` |
| `create_datagram_endpoint` | `UnsupportedOperation` (use `node.messages`) |
| `create_unix_connection`/`server`, pipes, `subprocess_*` | `UnsupportedOperation` / `NondeterminismError` |
| `start_tls`, `ssl=` | `UnsupportedOperation` |
| `sock_recv`/`sock_sendall`/`sock_accept`, `add_reader`/`add_writer`, `sendfile` | `UnsupportedOperation` |
| `add_signal_handler` | `UnsupportedOperation` |
| IPv6 | Not simulated (`gaierror`) |
| `loop.set_debug(True)` | Accepted; handles capture tracebacks |

Version-specific behaviour is mirrored where applications can observe it:
`SimServer.wait_closed()` follows 3.11 vs 3.12+ semantics, and
`serve_forever()` closes client connections on cancellation from 3.13.

## Private CPython internals used

Isolated in `src/detloop/_compat.py`, verified on 3.11–3.14:

| Internal | Why |
|---|---|
| `asyncio.events._set_running_loop` / `_get_running_loop` | Loop implementers' hook (listed in `events.__all__`) |
| `Handle._run()` | Runs a callback in its context and routes exceptions to the loop |
| `loop._timer_handle_cancelled(handle)` | Called by `TimerHandle.cancel()` on its loop |
| `TimerHandle._scheduled` | Maintained for `TimerHandle.__repr__` |
| `Task._log_destroy_pending` | Silences "destroyed but pending" for intentionally frozen tasks of crashed nodes |
| `__self__` on task step callbacks | Task completion events at full trace level |

## Errors you may see

| Error | Meaning |
|---|---|
| `NondeterminismError` | The code used an API outside the determinism boundary (sticky: the run fails even if caught) |
| `UnsupportedOperation` | An asyncio feature detloop does not simulate |
| `DeadlockError` | Nothing runnable, no timers, test not finished (lists blocked tasks) |
| `StarvationError` | Too many steps without virtual time advancing |
| `BudgetExceeded` | Steps, virtual time, buffer or teardown budget exhausted (run is *incomplete*) |
| `ReplayDivergence` / `ReplayMismatch` | Strict replay diverged / replay envelope mismatch |

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for fixes.
