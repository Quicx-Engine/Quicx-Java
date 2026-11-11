# Changelog

## 2.1.0 (2026-11-11)

For quicxd 2.1.0; everything else works with quicxd 1.0.3 and later as before.

- Value levels: `submit`, `submitAsync` and `submitAndWait` take a level from 0 to 15 and send `SUBMIT_VALUED` (0x0D). Higher levels reach workers first, and when quicxd's memory budget is full a task can push out a waiting task of a lower level. `QuicxClient.DEFAULT_LEVEL` (8) is what a plain submit gets; `QuicxClient.CRITICAL_LEVEL` (15) is never evicted. A level outside 0-15 is refused before anything is sent.
- `TaskResult.isEvicted()`: the task was evicted to make room for a more valuable one and never ran.
- `QuicxClient.Builder.maxBody(int)` for a quicxd built with `make MAX_PAYLOAD=N` (up to 16,320 bytes); the level byte counts toward the limit.
- `ErrorCode` documents what a quicxd older than 2.1 answers to a valued submit (`INVALID_MESSAGE`).
- Tests: 34 unit tests and 6 against a real quicxd (the 2 for levels and eviction skip themselves on quicxd older than 2.1). CI runs them on Java 11, 17 and 21 and against quicxd built from source.

## 2.0.0 (2026-11-10)

- `QuicxClient` keeps one connection open and reuses it, reconnecting when it breaks. One client can be shared between threads.
- New: `submitAndWait` and `submitAsync` return the task's result (DONE or FAILED, with the worker's reason).
- New: builders for both classes, with connect, response and heartbeat timeouts and the reconnect delay.
- New: `QuicxRejectedException` with an `ErrorCode` (`isQueueFull()` for a full memory budget) and `QuicxConnectionException`. All exceptions are unchecked.
- New: `handleTask` gives a handler the task's id and type, `fallback` takes tasks of types without a handler, and `startInBackground` / `stop` control the worker from another thread.
- The worker sends heartbeats while idle, reconnects with an increasing delay, lets a running task finish on `stop()`, and logs through `System.Logger` instead of printing.
- Task types and payloads are checked before sending; FAILED reasons are cut to the 63 bytes quicxd accepts (1.0 sent longer ones, which quicxd rejected).
- The jar has the module name `dev.quicx.client`.

## 1.0.0 (2026-04-10)

- First release: `QuicxClient.submit` and `QuicxWorker`.
