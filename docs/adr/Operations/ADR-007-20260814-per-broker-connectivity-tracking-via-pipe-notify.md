# ADR-007: Per-broker connectivity tracking via NNG pipe_notify

| Field        | Value |
|--------------|-------|
| **Category** | Operations |
| **Status**   | Accepted |
| **Created**  | 2026-08-14 08:30 |
| **Implemented** | [PR #19](https://github.com/fibonsai/cryptomeria-historic/pull/19) |

## Context

When the `receive_loop` detects a recv-timeout, the broker-down warning must
list **only the brokers that are actually disconnected** — not every configured
address. The subscriber uses a single NNG SUB socket that dials all configured
brokers, so a timeout on `recv()` does not indicate *which* broker is down.

We need a mechanism to introspect per-dialer pipe state from inside the
`NngSubscriber` wrapper.

## Options Considered

### Option 1: `nng` crate — `pipe_notify` callback (chosen)

The existing `nng` crate (v1.0.1) exposes `Socket::pipe_notify()`, which
delivers `PipeEvent::AddPost` / `RemovePost` events. Each `Pipe` can be
resolved to its owning `Dialer` via `Pipe::dialer()`, and the dialer URL is
readable via `Dialer::get_opt::<Url>()`.

- **Pros:** Direct NNG-level connectivity state; no external dependencies
  swapped; minimal architectural change; `dial_async()` (non-blocking) is used so
  construction succeeds even when a broker is unreachable.
- **Cons:** URL returned by `get_opt::<Url>()` may be canonified by the
  transport (e.g. hostname → IP), though for IP-based TCP URLs this is a no-op.

### Option 2: `anng` crate — async + `Message::remote_addr()`

`anng` (v0.2.0) is an async-first binding that returns a `Dialer` handle from
`dial()` and provides `Message::remote_addr()` to identify which peer a message
was received from.

- **Pros:** Native tokio integration; per-message source tracking.
- **Cons:** Pre-1.0 (unstable API); no `pipe_notify` equivalent —
  connectivity would have to be inferred from message activity, conflating
  "no data" with "disconnected"; requires full async rewrite of
  `receive_loop`.

### Option 3: Per-broker SUB sockets

Create one SUB socket per configured address, each on its own blocking
thread. The thread that times out identifies its broker as down.

- **Pros:** Simplest connectivity model.
- **Cons:** Significantly more sockets and threads; departs from the
  multiplexed-single-socket design documented in AGENTS.md.

## Decision

Adopt **Option 1**. Use `nng::Socket::pipe_notify()` with a
`Fn(Pipe, PipeEvent) + Send + Sync + 'static` callback that maintains an
`Arc<Mutex<HashMap<String, usize>>>` of per-URL active-pipe counts.

Key implementation details:

- `Socket::dial_async()` (non-blocking) is used instead of `Socket::dial()`
  so that `NngSubscriber::new()` does not fail when a broker is unreachable.
- Counts are pre-populated with all configured addresses at zero; only
  addresses that appear in the counts map (via `AddPost`) with a non-zero
  count are considered up.
- `NngSubscriber::down_addrs()` returns the subset of configured addresses
  whose count is still zero.

## Consequences

- The warning message now accurately reports only the disconnected brokers,
  e.g. `… may be down: tcp://127.0.0.1:14243` instead of listing all
  configured brokers.
- Hostname-based URLs may not match if NNG canonifies them; this edge case is
  documented but not handled in the initial implementation.
- `NngSubscriber::new()` no longer fails on a single unreachable broker;
  the dial is non-blocking and NNG retries internally.

## References

- NNG pipe event API: <https://nanomsg.github.io/nng/man/v1.2.2/nng_pipe_notify.3.html>
- NNG dial options (URL): <https://nanomsg.github.io/nng/man/v1.2.2/nng_dialer_get.3.html>
- Issue #17: Improve broker-down warning log to include broker addresses
