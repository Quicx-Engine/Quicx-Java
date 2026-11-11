# Quicx Java client

[![Maven Central](https://img.shields.io/maven-central/v/dev.quicx/quicx-client?label=Maven%20Central)](https://central.sonatype.com/artifact/dev.quicx/quicx-client)
[![CI](https://github.com/Quicx-Engine/Quicx-Java/actions/workflows/ci.yml/badge.svg)](https://github.com/Quicx-Engine/Quicx-Java/actions/workflows/ci.yml)
[![Java 11+](https://img.shields.io/badge/Java-11%2B-blue)](#)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

`dev.quicx:quicx-client` lets Java programs use [Quicx](https://quicx.dev), a task queue daemon for devices and servers with little memory. Quicx never goes past its memory budget, and when the budget is full it keeps the most valuable work. This library submits tasks to it, waits for their results and runs workers.

It has no dependencies, needs Java 11 or later, and talks to quicxd over [QXP](https://quicx.dev/docs/binary-protocol), the Quicx Protocol.

- [Install](#install)
- [Quick start](#quick-start)
- [Value levels: keep the most valuable work](#value-levels-keep-the-most-valuable-work)
- [When quicxd says no](#when-quicxd-says-no)
- [Waiting for results](#waiting-for-results)
- [Running workers](#running-workers)
- [Settings](#settings)
- [Limits and delivery](#limits-and-delivery)
- [Compatibility](#compatibility)
- [Building and testing](#building-and-testing)

## Install

Maven:

```xml
<dependency>
  <groupId>dev.quicx</groupId>
  <artifactId>quicx-client</artifactId>
  <version>2.1.0</version>
</dependency>
```

Gradle (Kotlin):

```kotlin
implementation("dev.quicx:quicx-client:2.1.0")
```

Gradle (Groovy):

```groovy
implementation 'dev.quicx:quicx-client:2.1.0'
```

You also need a running quicxd. On Linux or macOS: `curl -fsSL https://quicx.dev/install.sh | sh && quicx start`.

## Quick start

A producer:

```java
try (QuicxClient client = new QuicxClient("localhost", 16381)) {
    int id = client.submit("send_email", "{\"to\":\"ana@example.com\"}");
}
```

A worker:

```java
new QuicxWorker("localhost", 16381)
    .handle("send_email", payload -> mailer.send(new String(payload, StandardCharsets.UTF_8)))
    .start();   // blocks until stop() is called from another thread
```

A client keeps one connection to quicxd and uses it for every call; if the connection breaks, the next call opens a new one. One client can be shared by any number of threads, so create it once, not once per task.

## Value levels: keep the most valuable work

With quicxd 2.1.0 or later, every task can carry a value level from 0 to 15. Higher levels reach workers first. When quicxd's memory budget is full and a task arrives, quicxd can push out a waiting task of a lower level to make room, but only when its allocator proves that the freed memory fits the new task. Level 15 is never evicted.

```java
client.submit("frame", "/cam/0/000123.jpg", 4);                // an ordinary camera frame
client.submit("frame", "/cam/0/000124.jpg", 12);               // a frame worth keeping
client.submit("stop", "", QuicxClient.CRITICAL_LEVEL);         // never evicted

// without a level, a task gets QuicxClient.DEFAULT_LEVEL (8)
client.submit("report", reportJson);
```

The producer of an evicted task hears about it through the task's result:

```java
CompletableFuture<TaskResult> f = client.submitAsync("frame", ref, 4);
f.thenAccept(r -> {
    if (r.isEvicted()) log.info("frame {} was dropped for a more valuable one", r.taskId());
});
```

An evicted task never ran. Send it again later if it still matters.

## When quicxd says no

Everything that can go wrong is an unchecked exception:

| Exception | Meaning | What to do |
|---|---|---|
| `QuicxRejectedException`, `isQueueFull()` true | the memory budget is full and nothing less valuable could be evicted | send the task again later, or with a higher level |
| `QuicxRejectedException`, other `code()` | quicxd refused the message, e.g. `INVALID_MESSAGE` for a valued submit to quicxd older than 2.1 | see `ErrorCode` |
| `QuicxConnectionException` | quicxd could not be reached, or the connection broke before it answered | the task may or may not have been queued |
| `IllegalArgumentException` | the type, level or payload cannot be sent; thrown before anything is sent | fix the call |

```java
try {
    client.submit("frame", ref, 4);
} catch (QuicxRejectedException e) {
    if (e.isQueueFull()) retryLater(ref);
    else throw e;
}
```

## Waiting for results

```java
// block until a worker reports DONE or FAILED
TaskResult r = client.submitAndWait("resize", imageRef, Duration.ofSeconds(30));
if (r.isFailed()) log.warn("resize failed: {}", r.failureReason().get());

// or carry on and get the result later
CompletableFuture<TaskResult> later = client.submitAsync("report", reportJson);
```

Every submit method has a variant with a level: `submit(type, payload, level)`, `submitAsync(type, payload, level)` and `submitAndWait(type, payload, level, timeout)`.

## Running workers

```java
QuicxWorker worker = new QuicxWorker("localhost", 16381)
    .handle("send_email", payload -> mailer.send(new String(payload, StandardCharsets.UTF_8)))
    .handleTask("resize", task -> images.resize(task.id(), task.payload()))
    .fallback(task -> log.warn("no handler for {}", task.type()));

Thread t = worker.startInBackground();   // or start() to block the current thread
...
worker.stop();                           // a running task finishes and reports first
```

When a handler returns, the worker reports DONE; when it throws, it reports FAILED with the exception's class and message, cut to the 63 bytes QXP allows. A task whose type has no handler is reported FAILED, unless you set a `fallback`.

If quicxd restarts or the network drops, the worker reconnects on its own, waiting 0.5 s after the first failure and up to 30 s after repeated ones. An idle worker sends a heartbeat every 5 seconds; if nothing comes back for three intervals, the connection is treated as dead. The worker logs through `System.Logger` under `dev.quicx.QuicxWorker`, so it ends up wherever your logging goes (java.util.logging by default, SLF4J or Log4j through their `System.Logger` bridges).

## Settings

```java
QuicxClient client = QuicxClient.builder()
    .host("queue.internal")
    .port(16381)
    .connectTimeout(Duration.ofSeconds(2))
    .responseTimeout(Duration.ofSeconds(5))   // how long submit() waits for quicxd to accept
    .maxBody(8192)                            // only for a quicxd built with make MAX_PAYLOAD=8192
    .build();

QuicxWorker worker = QuicxWorker.builder()
    .host("queue.internal")
    .heartbeatInterval(Duration.ofSeconds(10))
    .reconnectDelay(Duration.ofSeconds(1), Duration.ofMinutes(1))
    .build();
```

## Limits and delivery

The client checks these before sending anything:

- A task type is 1 to 31 bytes of UTF-8.
- A QXP body is at most 1,024 bytes by default: one length byte, the type and the payload, plus one byte for the level in a valued submit. With a 10-byte type that leaves 1,013 bytes of payload. A quicxd built with `make MAX_PAYLOAD=N` takes up to N (at most 16,320); set `maxBody(N)` to match.
- A level is 0 to 15.
- A FAILED reason is at most 63 bytes; longer messages are cut at a character boundary.

Delivery is at-most-once. quicxd frees a task as soon as it hands it to a worker, so a task whose worker dies is lost, and nothing is reported back. For large data, put the data where your workers can read it and send its path or id as the payload.

## Compatibility

| | quicxd 1.0.3 to 2.0.x | quicxd 2.1.0 and later |
|---|---|---|
| submit, results, workers | yes | yes |
| value levels and `isEvicted()` | refused by quicxd (2.0.0 answers `INVALID_MESSAGE`) | yes |
| `maxBody` above 1,024 | no | with `make MAX_PAYLOAD=N` |

The tests run against quicxd 2.0.0 and 2.1.0. Code written for client 2.0 compiles and behaves the same with 2.1. Coming from 1.0: exceptions are unchecked, the client keeps one connection open (so it also receives results), the worker logs instead of printing and reconnects with an increasing delay, and `dev.quicx.internal` was rewritten (it was never meant to be used directly).

## Building and testing

```sh
cd quicx-java
mvn test                                  # 34 unit tests against a scripted stand-in for quicxd
QUICXD=/path/to/quicxd mvn test           # also 6 tests against a real daemon
```

The tests against a real daemon start their own quicxd on a free port. The two for value levels and eviction skip themselves on quicxd older than 2.1. CI runs the unit tests on Java 11, 17 and 21, and all of them against quicxd built from [the Quicx source](https://github.com/anastassow/Quicx).

```
quicx-java/src/main/java/dev/quicx/            the public API: QuicxClient, QuicxWorker, Task, TaskResult, exceptions
quicx-java/src/main/java/dev/quicx/internal/   QXP frames and bodies (not a public API)
quicx-java/src/test/java/dev/quicx/            unit tests, the scripted daemon, tests against quicxd
```

The jar's module name is `dev.quicx.client`.

## Links

- Quicx, the daemon: [github.com/anastassow/Quicx](https://github.com/anastassow/Quicx), [quicx.dev](https://quicx.dev)
- This library's documentation: [quicx.dev/docs/java-client](https://quicx.dev/docs/java-client)
- QXP, the protocol: [quicx.dev/docs/binary-protocol](https://quicx.dev/docs/binary-protocol)
- Changes: [CHANGELOG.md](CHANGELOG.md)

Issues and pull requests are welcome.

## Licence

MIT, see [LICENSE](LICENSE).
