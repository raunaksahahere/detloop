# 0005: Bounded WGL search with a three-valued result

## Context

Checking linearizability is NP-complete in general. Practical checkers
(Knossos, Porcupine) use Wing & Gong's search with Lowe's memoization and
exploit P-compositionality (checking keys independently). Incomplete answers
must never masquerade as "correct".

## Decision

- Iterative WGL over a doubly linked list of call/return entries ordered by
  a strictly increasing logical index; memoization of
  `(linearized-set bitmask, model state)`.
- Operations with status `ok` have a return entry at their completion;
  `info`/`pending` operations get a return entry after all real events, so
  they may linearize anywhere after their invocation — including after every
  observed operation, which is equivalent to "never happened". `fail`
  operations are excluded. Read-only operations with unknown output are
  dropped.
- Models may be nondeterministic: `step` returns every possible successor
  state (a timed-out compare-and-set may or may not have succeeded).
- Budgets (`max_steps`, `max_cache`) turn exhaustion into `unknown`;
  `assert_linearizable` raises `CheckIncomplete`, classified as
  *incomplete*, never as pass or fail.
- `KVModel` partitions by key; generic models are partitioned only if they
  implement `partition`.

## Validation

An independent brute-force oracle (`tests/oracle.py`, sharing no code)
enumerates subsets of indeterminate operations and every real-time-consistent
permutation. The checker agrees on 1,500 Hypothesis-generated histories and
on all 675 exhaustively enumerated two-operation histories.

## Consequences

- Worst-case exponential; the budget bounds wall time per check.
- Counterexamples report the longest linearized prefix and the blocking
  operation, not a minimal explanation.
