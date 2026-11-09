# Quicx Java client

`dev.quicx:quicx-client` lets Java programs use [Quicx](https://quicx.dev), a task queue daemon with a hard memory limit. `QuicxClient` submits tasks and, if you want, waits for their result. `QuicxWorker` takes tasks of the types you register and runs them. Both talk to quicxd over QXP, the Quicx Protocol.

The library has no dependencies and needs Java 11 or later. It works with Quicx 1.0.3 and later; 2.0.0 of the client was tested against quicxd 1.0.4 and 2.0.0.

## Adding it to a build

Maven:

```xml
<dependency>
  <groupId>dev.quicx</groupId>
  <artifactId>quicx-client</artifactId>
  <version>2.0.0</version>
</dependency>
```

Gradle:

```kotlin
implementation("dev.quicx:quicx-client:2.0.0")
```

## Submitting tasks

```java
try (QuicxClient client = new QuicxClient("localhost", 16381)) {
    // returns once quicxd has queued the task
    int id = client.submit("send_email", "{\"to\":\"ana@example.com\"}");

    // or wait until a worker has finished it
    TaskResult result = client.submitAndWait("resize", imageBytes, Duration.ofSeconds(30));
    if (result.isFailed()) {
        log.warn("resize failed: {}", result.failureReason().get());
    }

    // or get a future and carry on
    CompletableFuture<TaskResult> later = client.submitAsync("report", reportJson);
}
```

A client keeps one connection to quicxd and uses it for every call. If the connection breaks, the next call opens a new one. One client can be shared by any number of threads, so create it once, not once per task.

What can go wrong is unchecked:

- `QuicxRejectedException` means quicxd answered with an error. The common one is a full memory budget: `e.isQueueFull()` is true, and sending the task again later is the right reaction.
- `QuicxConnectionException` means quicxd could not be reached, or the connection broke before it answered. In that case the task may or may not have been queued.
- `IllegalArgumentException` is thrown before anything is sent when the task type or payload doesn't fit in a QXP frame (see the limits below).

## Running a worker

```java
QuicxWorker worker = new QuicxWorker("localhost", 16381)
    .handle("send_email", payload -> mailer.send(new String(payload, StandardCharsets.UTF_8)))
    .handleTask("resize", task -> images.resize(task.id(), task.payload()));

worker.start();   // blocks until worker.stop() is called from another thread
```

`startInBackground()` runs the same loop on a daemon thread and returns it, which is handy inside an application server.

When a handler returns, the worker reports DONE; when it throws, it reports FAILED with the exception's class and message, cut to the 63 bytes QXP allows. The producer receives either one. A task whose type has no handler is reported FAILED, unless you set a `fallback(...)`.

`stop()` lets a task that is already running finish and report its result before the worker disconnects. If quicxd restarts or the network drops, the worker reconnects on its own, waiting 0.5 s after the first failure and up to 30 s after repeated ones. An idle worker sends a heartbeat every 5 seconds, and if nothing comes back for three intervals the connection is treated as dead.

The worker logs through `System.Logger` under the name `dev.quicx.QuicxWorker`, so it ends up wherever your logging is configured (java.util.logging by default, or SLF4J/Log4j through their `System.Logger` bridges). It doesn't print to standard output.

## Settings

Both classes have builders for the defaults you may want to change:

```java
QuicxClient client = QuicxClient.builder()
    .host("queue.internal")
    .port(16381)
    .connectTimeout(Duration.ofSeconds(2))
    .responseTimeout(Duration.ofSeconds(5))    // how long submit() waits for quicxd to accept
    .build();

QuicxWorker worker = QuicxWorker.builder()
    .host("queue.internal")
    .heartbeatInterval(Duration.ofSeconds(10))
    .reconnectDelay(Duration.ofSeconds(1), Duration.ofMinutes(1))
    .build();
```

## Limits

These come from QXP and from quicxd, and the client checks them before sending:

- A task type is 1 to 31 bytes of UTF-8.
- A task's frame body is at most 1,024 bytes: one length byte, the type, then the payload. With a 10-byte type, the payload can be up to 1,013 bytes.
- A FAILED reason is at most 63 bytes; longer messages are cut at a character boundary.

Delivery is at-most-once. quicxd frees a task as soon as it hands it to a worker, so a task whose worker dies is lost and nothing is reported back. If a `submitAndWait` times out, that is one possible cause.

## Upgrading from 1.0

Code written for 1.0 keeps working in most cases. The differences:

- Exceptions are unchecked now. `submit` and `start` no longer declare `throws Exception`, so a `catch` for a specific checked exception around them (for example `IOException`) no longer compiles and can be removed.
- The client keeps one connection open instead of opening one per submit, so it also receives the DONE and FAILED that quicxd sends back.
- The worker logs instead of printing to standard output, reconnects with an increasing delay instead of every 3 seconds, and sends heartbeats.
- `dev.quicx.internal` was rewritten. It was never meant to be used directly.

## Building and testing

```sh
cd quicx-java
mvn test                                 # unit tests against a scripted stand-in for quicxd
QUICXD=/path/to/quicxd mvn test          # also runs the tests against a real daemon
```

## Licence

MIT, see [LICENSE](LICENSE).
