# ADR-008: Switch to per-broker SUB sockets for reliable disconnect logging

| Field        | Value |
|--------------|-------|
| **Category** | Operations |
| **Status**   | Accepted |
| **Implemented** | pending PR (#20) |
| **Created**  | 2026-08-14 23:45 |
| **Relates**  | Issue #17, Issue #20, ADR-007 |

## Context

The subscriber must log when an individual NNG PUB broker goes down. ADR-007
selected **Option 1**: a single `nng::Sub0` socket that dials all configured
brokers, with a `pipe_notify` callback maintaining per-URL pipe counts that are
read inside the receive-loop's `Error::TimedOut` branch
(`NngSubscriber::down_addrs()`).

This design has an observability gap described in issue #20:

- The recv-timeout is a **global** "no messages arrived at all" signal. It is
  only consulted when `Socket::recv()` times out, which only happens when *every*
  broker is silent.
- When broker A is healthy and broker B is disconnected, messages keep flowing
  from A, `recv()` never times out, and the warning branch never runs.
- Result: broker B's disconnect is **silently ignored** as long as A is alive.

The first broker being down is logged only because that case produces a
continuous, global timeout — not because the disconnect is actually detected.

## Options Considered

### Option 1: Keep single SUB socket + add proactive `pipe_notify` logging

Emit `log::warn!`/`log::info!` inside the `pipe_notify` callback on
`PipeEvent::RemovePost`/`AddPost`.

- **Pros:** Minimal change; reuses ADR-007 implementation.
- **Cons:** A single multiplexed socket still cannot attribute a *timeout* to a
  broker, and `pipe_notify` only reflects NNG-level pipe state (which may lag
  real application liveness). It also cannot satisfy the original use case of a
  broker that accepts the TCP connection but never sends data. Partial fix only.

### Option 2: Async binding (`anng`) with per-message source tracking

- **Pros:** Native tokio integration; `Message::remote_addr()` identifies the
  peer per message.
- **Cons:** Pre-1.0 API (see ADR-007 Option 2); no `pipe_notify`; requires a full
  async rewrite of `receive_loop`; out of scope for this fix.

### Option 3: Per-broker SUB sockets (chosen)

Create one `nng::Sub0` socket per configured address, each running on its own
`tokio::task::spawn_blocking` task (NNG `recv()` is blocking C FFI, so it must
not run on a plain tokio green thread). Each task owns exactly one dialer, so a
recv-timeout unambiguously identifies its own broker.

 - **Pros:** Correct per-broker disconnect attribution; a timeout on broker B is
   genuinely attributable to B; clean separation of connectivity state per broker.
 - **Cons:** One socket + one blocking task per broker (slightly more resources);
   deviates from the multiplexed-single-socket design documented in ADR-007 and
   AGENTS.md; `nng::Message` must be `Send` to cross the `mpsc` channel; the QuestDB
   consumer must stay a `std::thread` (see Concurrency model, because
   `BorrowedSender` is `!Send`).

## Decision

Adopt **Option 3**, as analysed in ADR-007 Option 3. Introduce a `BrokerReader`
(one `Socket` per address) in `src/subscriber.rs`; `NngSubscriber::run()` spawns
one `tokio::task::spawn_blocking` task per broker pushing a structured
`BrokerOutput { Message(nng::Message) | Down { addr } | Up { addr } }` enum into
a shared `std::sync::mpsc` channel. The receive loop in `src/main.rs` runs on a
dedicated `std::thread` (because `questdb::BorrowedSender` is `!Send`) consuming
that channel and **deriving all logging from events**: `Down` →
`log::warn!("[subscriber] broker \`{addr}\` down")`, `Up` →
`log::info!("[subscriber] broker \`{addr}\` recovered")`). Remove the
global-timeout `down_addrs()`/`format_broker_down_warning` path in favor of
per-broker attribution.

### Concurrency model

`nng::Socket::recv()` is blocking C FFI and therefore **must not** run on a plain
tokio green thread (it would stall the reactor). It runs inside
`tokio::task::spawn_blocking` (tokio's managed blocking-thread pool — no manual
`std::thread::spawn`), so NNG's blocking recv is isolated from the reactor.

The **QuestDB writer** is different: `questdb-rs` `BorrowedSender` is `!Send`
(it carries `PhantomData<Rc<()>>` in `SenderHandle`), so the channel consumer
that calls `db.borrow_sender()` **cannot** be a `tokio::spawn` async task (tasks
migrate across threads). It therefore stays a dedicated `std::thread::spawn`,
mirroring the original design. Broker tasks push `BrokerOutput` events onto a
`std::sync::mpsc` channel (`Sender` is `Send + Sync`, `nng::Message` is `Send`);
the consumer drains it with `recv_timeout`.

### Down/up attribution & debounce

Each `BrokerReader` tracks down/up via `connectivity_event`, driven by the recv
result and the `pipe_notify` pipe count. A single `recv` timeout is **not**
treated as "down" (it can fire during the initial SUB/PUB handshake when the
pipe count is briefly 0, or during bursty pauses with the pipe still up).
Instead, `Down` is emitted only after `DOWN_THRESHOLD_TICKS` (2) consecutive
no-pipe timeouts (~1 s at 500 ms/timeout); `Up` is emitted immediately on a
pipe reconnect or a received message. This was validated as the root cause of
initial test flakiness under `cargo test --all-targets`.

### Testing

End-to-end coverage uses three in-process mock NNG `Pub0` sockets bound to
`tcp://127.0.0.1:0` (localhost ephemeral ports) — hermetic, no Docker, no
external network (see `tests/subscriber_broker_test.rs`). Each mocks a broker:
one is closed mid-run while the other two keep publishing; the test asserts the
`BrokerOutput::Down { addr: <closed> }` event fires **while** `BrokerOutput::Message`
events continue flowing from the surviving brokers — exactly the regression #20
describes. The structured-event channel makes assertions deterministic, avoiding
fragile `RUST_LOG`/`env_logger` capture.

## Consequences

- Brokers are now reported down/recovered independently, regardless of the
  connectivity state of other brokers.
- Each broker uses one extra `nng::Socket` and one blocking tokio task; for the
  modest number of configured brokers this is acceptable.
- The single-multiplexed-socket model from ADR-007 is superseded for the
  subscriber path; ADR-007 is retained for history.
- A broker that accepts a TCP connection but never sends data will still be
  detected as "down" via the per-broker recv-timeout, which is the desired
  application-level liveness signal.
- Tests depend on the `nng` crate being able to create/listen a `Pub0` socket
  within the test process; this is satisfied by the vendored NNG C library built
  at compile time by the `nng` crate.

## References

- NNG pipe event API: <https://nanomsg.github.io/nng/man/v1.2.2/nng_pipe_notify.3.html>
- NNG dial options (URL): <https://nanomsg.github.io/nng/man/v1.2.2/nng_dialer_get.3.html>
- ADR-007: Per-broker connectivity tracking via pipe_notify
- Issue #17: Improve broker-down warning log to include broker addresses
- Issue #20: Log per-broker disconnects; timeout-only detection misses secondary brokers
