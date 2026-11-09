package dev.quicx;

import dev.quicx.internal.Frame;
import dev.quicx.internal.Qxp;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class QuicxClientTest {

    @Test
    void submitSendsTheFrameAndReturnsTheId() throws Exception {
        try (FakeDaemon d = new FakeDaemon(p -> {
                 Frame f = p.read();
                 assertEquals(Qxp.SUBMIT, f.type);
                 assertArrayEquals(Qxp.submitBody("email", "hi".getBytes(StandardCharsets.UTF_8)), f.body);
                 p.ok(42);
                 p.awaitClose();
             });
             QuicxClient c = new QuicxClient("127.0.0.1", d.port())) {
            assertEquals(42, c.submit("email", "hi"));
            assertTrue(d.errors.isEmpty(), d.errors.toString());
        }
    }

    @Test
    void submitsShareOneConnection() throws Exception {
        try (FakeDaemon d = new FakeDaemon(p -> {
                 for (int i = 1; ; i++) {
                     p.read();
                     p.ok(i);
                 }
             });
             QuicxClient c = new QuicxClient("127.0.0.1", d.port())) {
            assertEquals(1, c.submit("a", "x"));
            assertEquals(2, c.submit("a", "x"));
            assertEquals(3, c.submit("a", new byte[0]));
            assertEquals(1, d.peers.size());
        }
    }

    @Test
    void aFullQueueIsARejection() throws Exception {
        try (FakeDaemon d = new FakeDaemon(p -> {
                 p.read();
                 p.error(0x01, "out of memory");
                 p.awaitClose();
             });
             QuicxClient c = new QuicxClient("127.0.0.1", d.port())) {
            QuicxRejectedException e = assertThrows(QuicxRejectedException.class, () -> c.submit("a", "x"));
            assertTrue(e.isQueueFull());
            assertEquals(ErrorCode.QUEUE_FULL, e.code());
            assertEquals("out of memory", e.reason());
        }
    }

    @Test
    void submitAndWaitReportsDoneAndFailed() throws Exception {
        try (FakeDaemon d = new FakeDaemon(p -> {
                 p.read();
                 p.ok(7);
                 p.done(7);
                 p.read();
                 p.ok(8);
                 p.failed(8, "disk full");
                 p.awaitClose();
             });
             QuicxClient c = new QuicxClient("127.0.0.1", d.port())) {
            TaskResult ok = c.submitAndWait("a", "x", Duration.ofSeconds(5));
            assertTrue(ok.isDone());
            assertEquals(7, ok.taskId());
            TaskResult bad = c.submitAndWait("a", "x", Duration.ofSeconds(5));
            assertTrue(bad.isFailed());
            assertEquals("disk full", bad.failureReason().orElseThrow());
        }
    }

    @Test
    void resultsAreMatchedByTaskIdNotByOrder() throws Exception {
        try (FakeDaemon d = new FakeDaemon(p -> {
                 p.read();
                 p.ok(1);
                 p.read();
                 p.ok(2);
                 p.done(2);
                 p.failed(1, "late");
                 p.awaitClose();
             });
             QuicxClient c = new QuicxClient("127.0.0.1", d.port())) {
            CompletableFuture<TaskResult> first = c.submitAsync("a", "1");
            CompletableFuture<TaskResult> second = c.submitAsync("a", "2");
            assertTrue(second.get(5, TimeUnit.SECONDS).isDone());
            TaskResult r1 = first.get(5, TimeUnit.SECONDS);
            assertEquals(1, r1.taskId());
            assertEquals("late", r1.failureReason().orElseThrow());
        }
    }

    @Test
    void aBrokenConnectionFailsWhatIsWaitingAndTheNextSubmitReconnects() throws Exception {
        try (FakeDaemon d = new FakeDaemon(
                 p -> {
                     p.read();
                     p.close();   // gone before answering
                 },
                 p -> {
                     p.read();
                     p.ok(5);
                     p.awaitClose();
                 });
             QuicxClient c = new QuicxClient("127.0.0.1", d.port())) {
            assertThrows(QuicxConnectionException.class, () -> c.submit("a", "x"));
            assertEquals(5, c.submit("a", "x"));
            assertEquals(2, d.peers.size());
        }
    }

    @Test
    void aBadTaskIsRefusedBeforeConnecting() throws Exception {
        try (FakeDaemon d = new FakeDaemon(FakeDaemon.Peer::awaitClose);
             QuicxClient c = new QuicxClient("127.0.0.1", d.port())) {
            assertThrows(IllegalArgumentException.class, () -> c.submit("t".repeat(32), "x"));
            assertThrows(IllegalArgumentException.class, () -> c.submit("t", new byte[Qxp.MAX_BODY]));
            FakeDaemon.sleep(100);
            assertEquals(0, d.peers.size());
        }
    }

    @Test
    void noDaemonIsAConnectionError() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        try (QuicxClient c = new QuicxClient("127.0.0.1", port)) {
            assertThrows(QuicxConnectionException.class, () -> c.submit("a", "x"));
        }
    }

    @Test
    void noAnswerTimesOut() throws Exception {
        try (FakeDaemon d = new FakeDaemon(FakeDaemon.Peer::awaitClose);
             QuicxClient c = QuicxClient.builder().host("127.0.0.1").port(d.port())
                     .responseTimeout(Duration.ofMillis(200)).build()) {
            long t0 = System.nanoTime();
            assertThrows(QuicxConnectionException.class, () -> c.submit("a", "x"));
            assertTrue(System.nanoTime() - t0 < TimeUnit.SECONDS.toNanos(3));
        }
    }

    @Test
    void closeFailsResultsStillWaiting() throws Exception {
        try (FakeDaemon d = new FakeDaemon(p -> {
            p.read();
            p.ok(1);
            p.awaitClose();
        })) {
            QuicxClient c = new QuicxClient("127.0.0.1", d.port());
            CompletableFuture<TaskResult> f = c.submitAsync("a", "x");
            FakeDaemon.sleep(100);
            c.close();
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
            assertInstanceOf(QuicxConnectionException.class, e.getCause());
            assertThrows(QuicxException.class, () -> c.submit("a", "x"));
        }
    }

    @Test
    void manyThreadsCanShareOneClient() throws Exception {
        try (FakeDaemon d = new FakeDaemon(p -> {
                 for (int i = 1; ; i++) {
                     p.read();
                     p.ok(i);
                     p.done(i);
                 }
             });
             QuicxClient c = new QuicxClient("127.0.0.1", d.port())) {
            Thread[] threads = new Thread[8];
            java.util.concurrent.atomic.AtomicInteger doneCount = new java.util.concurrent.atomic.AtomicInteger();
            for (int t = 0; t < threads.length; t++) {
                threads[t] = new Thread(() -> {
                    for (int i = 0; i < 200; i++)
                        if (c.submitAndWait("a", "x", Duration.ofSeconds(5)).isDone()) doneCount.incrementAndGet();
                });
                threads[t].start();
            }
            for (Thread t : threads) t.join();
            assertEquals(1600, doneCount.get());
            assertEquals(1, d.peers.size());
        }
    }
}
