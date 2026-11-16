# 0006: Decision tapes, strict replay, execution-validated shrinking

## Context

"Replay from a seed" only works when code, configuration and runtime match.
Shrinking a failure by deleting random draws is unsound: removing one draw
shifts every later draw of the same generator, so the "smaller" schedule is a
different execution that may not fail at all.

## Decision

- Every random decision is an integer choice in `[0, n)` on one of several
  independent streams, recorded on a per-stream tape as `(n, value)`. Value 0
  is the least eventful option.
- Replay modes: seed replay (regenerate and compare the trace hash), strict
  tape replay (every decision must offer the recorded `n`; otherwise
  `ReplayDivergence`), guided (shrinker hints).
- Manifests record the replay envelope (workload and config fingerprints,
  versions, hash seed) and replay refuses or flags mismatches.
- The shrinker runs ddmin over the explicit fault list, then for each tape
  stream (scheduling, network, workload): prefix truncation and ddmin over
  non-zero positions. **Every candidate is executed** in guided mode and
  accepted only if it fails with the original signature. Candidates get step
  and virtual-time budgets derived from the original failure, because a much
  longer run is not "smaller".
- The result is re-executed in strict mode and must reproduce the candidate's
  trace hash exactly before it is saved.
- Status is reported as `fixed_point` (a full round with no progress: 1-minimal
  for these transformations) or `budget_exhausted`; global minimality is never
  claimed.

## Consequences

- Shrinking costs one full simulation per candidate. On the KV corpus,
  zeroing network decisions can make runs longer (faster messages → more
  operations), so the shrinker often stops on its budget for that stream;
  this is reported, not hidden.
- Pinned seeds are only stable within a runtime family (ADR 0007).
