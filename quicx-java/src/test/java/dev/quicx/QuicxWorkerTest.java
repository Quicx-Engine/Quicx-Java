package dev.quicx;

import dev.quicx.internal.Frame;
import dev.quicx.internal.Qxp;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class QuicxWorkerTest {

    private static QuicxWorker worker(int port) {
        return QuicxWorker.builder().host("127.0.0.1").port(port)
                .reconnectDelay(Duration.ofMillis(10), Duration.ofMillis(50)).build();
    }

    @Test
    void registersRunsATaskAndReportsDone() throws Exception {
        LinkedBlockingQueue<Frame> fromWorker = new LinkedBlockingQueue<>();
        try (FakeDaemon d = new FakeDaemon(p -> {
            fromWorker.add(p.expect(Qxp.READY));
            p.task(5, "email", "hello".getBytes(StandardCharsets.UTF_8));
            fromWorker.add(p.expect(Qxp.DONE));
            p.awaitClose();
        })) {
            LinkedBlockingQueue<String> seen = new LinkedBlockingQueue<>();
            QuicxWorker w = worker(d.port()).handle("email", payload -> seen.add(new String(payload, StandardCharsets.UTF_8)));
            Thread t = w.startInBackground();

            assertEquals(Qxp.READY, fromWorker.poll(5, TimeUnit.SECONDS).type);
            assertEquals("hello", seen.poll(5, TimeUnit.SECONDS));
            Frame done = fromWorker.poll(5, TimeUnit.SECONDS);
            assertEquals(5, Qxp.getInt(done.body, 0));
            assertEquals(1, w.tasksDone());

            w.stop();
            t.join(5000);
            assertFalse(t.isAlive());
        }
    }

    @Test
    void anExceptionIsReportedFailedWithAShortReason() throws Exception {
        LinkedBlockingQueue<Frame> fromWorker = new LinkedBlockingQueue<>();
        try (FakeDaemon d = new FakeDaemon(p -> {
            p.expect(Qxp.READY);
            p.task(9, "resize", new byte[0]);
            fromWorker.add(p.expect(Qxp.FAILED));
            p.awaitClose();
        })) {
            QuicxWorker w = worker(d.port()).handleTask("resize", task -> {
                throw new IllegalStateException("image " + task.id() + " is broken: " + "x".repeat(200));
            });
            w.startInBackground();
            Frame f = fromWorker.poll(5, TimeUnit.SECONDS);
            assertEquals(9, Qxp.getInt(f.body, 0));
            String reason = Qxp.utf8(f.body, 4, f.body.length - 4);
            assertTrue(reason.startsWith("IllegalStateException: image 9 is broken"), reason);
            assertTrue(f.body.length - 4 <= Qxp.MAX_REASON_BYTES);
            assertEquals(1, w.tasksFailed());
            w.stop();
        }
    }

    @Test
    void aTypeWithoutAHandlerFailsUnlessThereIsAFallback() throws Exception {
        LinkedBlockingQueue<Frame> fromWorker = new LinkedBlockingQueue<>();
        try (FakeDaemon d = new FakeDaemon(p -> {
            p.expect(Qxp.READY);
            p.task(1, "unknown", new byte[0]);
            fromWorker.add(p.expect(Qxp.FAILED));
            p.awaitClose();
        })) {
            QuicxWorker w = worker(d.port()).handle("email", payload -> { });
            w.startInBackground();
            Frame f = fromWorker.poll(5, TimeUnit.SECONDS);
            assertEquals("no handler for task type unknown", Qxp.utf8(f.body, 4, f.body.length - 4));
            w.stop();
        }
        try (FakeDaemon d = new FakeDaemon(p -> {
            p.expect(Qxp.READY);
            p.task(2, "unknown", new byte[0]);
            fromWorker.add(p.expect(Qxp.DONE));
            p.awaitClose();
        })) {
            LinkedBlockingQueue<Task> seen = new LinkedBlockingQueue<>();
            QuicxWorker w = worker(d.port()).fallback(seen::add);
            w.startInBackground();
            assertEquals(Qxp.DONE, fromWorker.poll(5, TimeUnit.SECONDS).type);
            assertEquals("unknown", seen.poll(5, TimeUnit.SECONDS).type());
            w.stop();
        }
    }

    @Test
    void reconnectsWhenTheDaemonGoesAwayOrRefusesIt() throws Exception {
        CountDownLatch thirdConnection = new CountDownLatch(1);
        try (FakeDaemon d = new FakeDaemon(
                p -> {
                    p.expect(Qxp.READY);
                    p.close();                          // daemon restart
                },
                p -> {
                    p.expect(Qxp.READY);
                    p.error(0x01, "worker pool full");  // no room for this worker
                    p.awaitClose();
                },
                p -> {
                    p.expect(Qxp.READY);
                    thirdConnection.countDown();
                    p.awaitClose();
                })) {
            QuicxWorker w = worker(d.port()).handle("a", payload -> { });
            w.startInBackground();
            assertTrue(thirdConnection.await(5, TimeUnit.SECONDS));
            assertEquals(3, d.peers.size());
            w.stop();
        }
    }

    @Test
    void anIdleWorkerSendsHeartbeats() throws Exception {
        CountDownLatch heartbeat = new CountDownLatch(1);
        try (FakeDaemon d = new FakeDaemon(p -> {
            p.expect(Qxp.READY);
            Frame f = p.read();
            if (f.type == Qxp.HEARTBEAT) heartbeat.countDown();
            p.send(Qxp.PONG, null);
            p.awaitClose();
        })) {
            QuicxWorker w = QuicxWorker.builder().host("127.0.0.1").port(d.port())
                    .heartbeatInterval(Duration.ofMillis(100)).build().handle("a", payload -> { });
            w.startInBackground();
            assertTrue(heartbeat.await(5, TimeUnit.SECONDS));
            w.stop();
        }
    }

    @Test
    void stopLetsTheRunningTaskFinishAndReportFirst() throws Exception {
        LinkedBlockingQueue<Frame> fromWorker = new LinkedBlockingQueue<>();
        CountDownLatch started = new CountDownLatch(1);
        try (FakeDaemon d = new FakeDaemon(p -> {
            p.expect(Qxp.READY);
            p.task(3, "slow", new byte[0]);
            fromWorker.add(p.expect(Qxp.DONE));
            p.awaitClose();
        })) {
            QuicxWorker w = worker(d.port()).handle("slow", payload -> {
                started.countDown();
                Thread.sleep(300);
            });
            Thread t = w.startInBackground();
            assertTrue(started.await(5, TimeUnit.SECONDS));
            w.stop();
            assertEquals(3, Qxp.getInt(fromWorker.poll(5, TimeUnit.SECONDS).body, 0));
            t.join(5000);
            assertFalse(t.isAlive());
        }
    }

    @Test
    void aWorkerStartsOnce() throws Exception {
        try (FakeDaemon d = new FakeDaemon(FakeDaemon.Peer::awaitClose)) {
            QuicxWorker w = worker(d.port());
            w.startInBackground();
            FakeDaemon.sleep(100);
            assertThrows(IllegalStateException.class, w::start);
            w.stop();
        }
    }
}
