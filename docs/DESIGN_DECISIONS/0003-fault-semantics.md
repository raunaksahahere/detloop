# 0003: Reliable streams with faults below them; separate message faults

## Context

The brief lists loss, duplication and reordering as network faults. Exposing
them on a TCP-like stream would violate the stream contract applications rely
on: TCP never shows duplicated or reordered bytes. Applications do, however,
see stalls, resets and timeouts.

## Decision

- Streams are reliable and ordered. Faults act below the stream: a partition
  makes segments lost on arrival; the direction stalls (later segments are
  held to preserve order) and the sender retransmits once the link is usable;
  after `retransmit_timeout` the sender aborts with `TimeoutError`. Resets,
  connection refusal, connect timeouts and crash-induced resets complete the
  model. Flow control is end-to-end: receiver window, write buffer high/low
  water marks, `drain()`.
- Application-visible loss, duplication and reordering live in a separate
  message bus (`node.messages(port)`), enabled by the `messages` fault kind.
- Datagram endpoints (`create_datagram_endpoint`) are not implemented in
  0.1.0 and raise `UnsupportedOperation` rather than being advertised.
- Two crash flavours model the difference between a process crash (the OS
  resets connections) and a host disappearance (silence until timeout).

## Alternatives

- **Full TCP state machine** (SACK, congestion control, TIME_WAIT): far more
  code, little additional bug-finding power for application-level tests.
- **Lossy streams**: would surface impossible behaviours.

## Consequences

- Acknowledgements are instantaneous at delivery; no congestion control. Bulk
  throughput numbers are not realistic network models.
- Half-open connections exist (the receiver of a stalled direction does not
  learn about the sender's timeout until it sends), as in real TCP.
