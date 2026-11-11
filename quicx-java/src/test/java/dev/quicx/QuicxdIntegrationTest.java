package dev.quicx;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs against a real quicxd. Skipped unless the environment variable
 * QUICXD names the binary, for example:
 *
 * <pre>QUICXD=/path/to/quicxd mvn test</pre>
 */
class QuicxdIntegrationTest {

    private static Process daemon;
    private static int port;

    @BeforeAll
    static void startDaemon() throws Exception {
        String bin = System.getenv("QUICXD");
        assumeTrue(bin != null && new File(bin).canExecute(), "set QUICXD to a quicxd binary to run these tests");
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        File log = File.createTempFile("quicxd-test", ".log");
        daemon = new ProcessBuilder(bin, "start", "--port", String.valueOf(port))
                .redirectErrorStream(true).redirectOutput(log).start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("127.0.0.1", port), 200);
                return;
            } catch (Exception e) {
                if (System.nanoTime() > deadline || !daemon.isAlive()) throw new IllegalStateException("quicxd did not start, see " + log);
                Thread.sleep(50);
            }
        }
    }

    @AfterAll
    static void stopDaemon() throws Exception {
        if (daemon != null) {
            daemon.destroy();
            daemon.waitFor(5, TimeUnit.SECONDS);
        }
    }

    /** True if the quicxd under test is at least major.minor ("quicx v2.1.0" from `quicxd version`). */
    private static boolean daemonAtLeast(int major, int minor) throws Exception {
        Process p = new ProcessBuilder(System.getenv("QUICXD"), "version").redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        p.waitFor(5, TimeUnit.SECONDS);
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("v(\\d+)\\.(\\d+)").matcher(out);
        if (!m.find()) return false;
        int ma = Integer.parseInt(m.group(1)), mi = Integer.parseInt(m.group(2));
        return ma > major || (ma == major && mi >= minor);
    }

    /** A second daemon with a small budget, for filling it quickly. */
    private static Process startSmallDaemon(int smallPort) throws Exception {
        File conf = File.createTempFile("quicxd-small", ".conf");
        java.nio.file.Files.writeString(conf.toPath(), "[server]\nport = " + smallPort + "\n\n[memory]\npool_size = 256K\n");
        File log = File.createTempFile("quicxd-small", ".log");
        Process d = new ProcessBuilder(System.getenv("QUICXD"), "start", "--config", conf.getPath())
                .redirectErrorStream(true).redirectOutput(log).start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress("127.0.0.1", smallPort), 200);
                return d;
            } catch (Exception e) {
                if (System.nanoTime() > deadline || !d.isAlive()) throw new IllegalStateException("small quicxd did not start, see " + log);
                Thread.sleep(50);
            }
        }
    }

    private static QuicxWorker worker() {
        return QuicxWorker.builder().host("127.0.0.1").port(port)
                .reconnectDelay(Duration.ofMillis(50), Duration.ofMillis(200)).build();
    }

    @Test
    void aTaskGoesThroughAWorkerAndTheResultComesBack() throws Exception {
        LinkedBlockingQueue<Task> seen = new LinkedBlockingQueue<>();
        QuicxWorker w = worker().handleTask("it-echo", seen::add)
                .handle("it-fail", payload -> { throw new IllegalArgumentException("bad input"); });
        w.startInBackground();
        try (QuicxClient c = new QuicxClient("127.0.0.1", port)) {
            TaskResult ok = c.submitAndWait("it-echo", "héllo", Duration.ofSeconds(10));
            assertTrue(ok.isDone(), ok.toString());
            Task t = seen.poll(5, TimeUnit.SECONDS);
            assertEquals(ok.taskId(), t.id());
            assertEquals("héllo", t.payloadAsString());

            TaskResult bad = c.submitAndWait("it-fail", "x", Duration.ofSeconds(10));
            assertTrue(bad.isFailed());
            assertEquals("IllegalArgumentException: bad input", bad.failureReason().orElseThrow());
        } finally {
            w.stop();
        }
    }

    @Test
    void theLargestPayloadFits() throws Exception {
        LinkedBlockingQueue<Integer> lengths = new LinkedBlockingQueue<>();
        QuicxWorker w = worker().handleTask("it-big", t -> lengths.add(t.payloadLength()));
        w.startInBackground();
        try (QuicxClient c = new QuicxClient("127.0.0.1", port)) {
            int max = 1024 - 1 - "it-big".length();
            byte[] payload = new byte[max];
            for (int i = 0; i < max; i++) payload[i] = (byte) i;
            assertTrue(c.submitAndWait("it-big", payload, Duration.ofSeconds(10)).isDone());
            assertEquals(max, lengths.poll(5, TimeUnit.SECONDS));
            assertThrows(IllegalArgumentException.class, () -> c.submit("it-big", new byte[max + 1]));
        } finally {
            w.stop();
        }
    }

    @Test
    void aThousandTasksAllComeBack() throws Exception {
        QuicxWorker w1 = worker().handle("it-many", payload -> { });
        QuicxWorker w2 = worker().handle("it-many", payload -> { });
        w1.startInBackground();
        w2.startInBackground();
        try (QuicxClient c = new QuicxClient("127.0.0.1", port)) {
            List<CompletableFuture<TaskResult>> all = new ArrayList<>();
            for (int i = 0; i < 1000; i++) all.add(c.submitAsync("it-many", "task " + i));
            CompletableFuture.allOf(all.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
            for (CompletableFuture<TaskResult> f : all) assertTrue(f.get().isDone());
            assertEquals(1000, w1.tasksDone() + w2.tasksDone());
        } finally {
            w1.stop();
            w2.stop();
        }
    }

    @Test
    void aValuedTaskReachesAWorker() throws Exception {
        assumeTrue(daemonAtLeast(2, 1), "value levels need quicxd 2.1.0");
        LinkedBlockingQueue<Task> seen = new LinkedBlockingQueue<>();
        QuicxWorker w = worker().handleTask("it-valued", seen::add);
        w.startInBackground();
        try (QuicxClient c = new QuicxClient("127.0.0.1", port)) {
            TaskResult r = c.submitAndWait("it-valued", "frame 1", 12, Duration.ofSeconds(10));
            assertTrue(r.isDone(), r.toString());
            assertEquals("frame 1", seen.poll(5, TimeUnit.SECONDS).payloadAsString());
            assertTrue(c.submitAndWait("it-valued", "stop", QuicxClient.CRITICAL_LEVEL, Duration.ofSeconds(10)).isDone());
        } finally {
            w.stop();
        }
    }

    @Test
    void aFullBudgetEvictsALessValuableTaskForAMoreValuableOne() throws Exception {
        assumeTrue(daemonAtLeast(2, 1), "eviction needs quicxd 2.1.0");
        int smallPort;
        try (ServerSocket s = new ServerSocket(0)) {
            smallPort = s.getLocalPort();
        }
        Process small = startSmallDaemon(smallPort);
        try (QuicxClient c = new QuicxClient("127.0.0.1", smallPort)) {
            byte[] payload = new byte[40];
            List<CompletableFuture<TaskResult>> low = new ArrayList<>();
            for (int i = 0; i < 2500; i++) low.add(c.submitAsync("frame", payload, 1));

            // one more of the same level: there is nothing less valuable to evict, so it is refused.
            // Answers come in order, so once this one is back, all of the above have been answered too.
            QuicxRejectedException full = assertThrows(QuicxRejectedException.class, () -> c.submit("frame", payload, 1));
            assertTrue(full.isQueueFull(), full.toString());
            long refused = low.stream().filter(CompletableFuture::isCompletedExceptionally).count();
            assertTrue(refused > 0, "the 256 KiB budget never filled");

            // a more valuable task gets in by evicting the oldest level-1 task, whose producer is told
            c.submit("frame", payload, 12);
            CompletableFuture<TaskResult> firstAccepted = low.stream()
                    .filter(f -> !f.isCompletedExceptionally()).findFirst().orElseThrow();
            TaskResult evicted = firstAccepted.get(5, TimeUnit.SECONDS);
            assertTrue(evicted.isEvicted(), evicted.toString());
        } finally {
            small.destroy();
            small.waitFor(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void aFullBudgetIsAQueueFullRejectionAndDrainsAfterwards() throws Exception {
        byte[] payload = new byte[900];
        int accepted = 0;
        try (QuicxClient c = new QuicxClient("127.0.0.1", port)) {
            QuicxRejectedException refused = null;
            while (refused == null && accepted < 200_000) {
                try {
                    c.submit("it-fill", payload);
                    accepted++;
                } catch (QuicxRejectedException e) {
                    refused = e;
                }
            }
            assertNotNull(refused, "the budget never filled");
            assertTrue(refused.isQueueFull(), refused.toString());
            assertTrue(accepted > 100, "only " + accepted + " tasks of 900 B fit");

            QuicxWorker w = worker().handle("it-fill", p -> { });
            w.startInBackground();
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (w.tasksDone() < accepted && System.nanoTime() < deadline) Thread.sleep(20);
                assertEquals(accepted, w.tasksDone());
                assertTrue(c.submitAndWait("it-fill", "after".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(10)).isDone());
            } finally {
                w.stop();
            }
        }
    }
}
