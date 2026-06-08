---
title: Testing
description: How the SDK is tested, and how to run the suites.
---

# Testing

The SDK ships two test suites: fast **unit tests** (no server) and **integration tests** that run
real workflows against a Temporal dev server.

## Unit tests

18 GoogleTest cases cover the data converter, the coroutine primitive (including
teardown-unwinds-a-suspended-stack), and the workflow runtime seam (futures, signal channels,
selectors, cancellation) against a fake environment. They need no server:

```bash
ctest --test-dir build -LE integration
```

## Integration tests

17 end-to-end cases run against a real dev server and are gated behind `TEMPORAL_INTEGRATION=1`, so
the default run stays server-free:

```bash
temporal server start-dev &
TEMPORAL_INTEGRATION=1 ctest --test-dir build -L integration
```

They exercise the real engine end-to-end:

- timer, single + parallel activities, activity-failure propagation, `RetryPolicy` fail-fast
- terminate, signal delivery + ordering, observed cancellation
- live-state query, state-accumulating update
- selector (activity wins / timeout wins), child workflow
- sticky-cache continuations, activity heartbeating, continue-as-new chaining

## CI

`.github/workflows/ci.yml` runs both suites on macOS: it installs dependencies, builds, runs the
unit tests, then stands up a dev server and runs the integration suite.

## A note on flakiness

Some Temporal interactions are *eventually* consistent (e.g. a query issued immediately after a
signal). Tests that read-after-write poll until the expected state is visible, rather than asserting
once — the right pattern for eventually-consistent reads.

## What's not here

A first-class **test framework** (an in-memory test environment with time-skipping, and a
deterministic **replayer** for recorded histories) is a major feature of the official SDKs that this
project does not yet provide. See the [parity matrix](/parity).
