# Changelog

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
