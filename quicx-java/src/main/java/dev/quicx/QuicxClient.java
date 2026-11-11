package dev.quicx;

import dev.quicx.internal.Frame;
import dev.quicx.internal.Qxp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Submits tasks to quicxd.
 *
 * <p>One client keeps one TCP connection open and reuses it for every
 * submit; if the connection breaks, the next call opens a new one. It is
 * safe to share one client between threads, and cheaper than creating one
 * per task.
 *
 * <pre>{@code
 * try (QuicxClient client = new QuicxClient("localhost", 16381)) {
 *     int id = client.submit("send_email", "{\"to\":\"ana@example.com\"}");
 *
 *     TaskResult r = client.submitAndWait("resize", imageBytes, Duration.ofSeconds(30));
 *     if (r.isFailed()) log(r.failureReason().get());
 *
 *     // quicxd 2.1+: a value level from 0 to 15; when the budget is full, a
 *     // more valuable task can push out a less valuable one that is waiting
 *     client.submit("frame", frameRef, 12);
 *     client.submit("stop", "", QuicxClient.CRITICAL_LEVEL);   // never evicted
 * }
 * }</pre>
 *
 * <p>Errors are unchecked: {@link QuicxRejectedException} when quicxd says
 * no (its memory budget is full, for instance), {@link QuicxConnectionException}
 * when it cannot be reached, and {@link IllegalArgumentException} for a task
 * type or payload that QXP cannot carry.
 */
public final class QuicxClient implements AutoCloseable {

    /** The port quicxd listens on unless configured otherwise. */
    public static final int DEFAULT_PORT = 16381;

    /** The value level quicxd gives a task submitted without one. */
    public static final int DEFAULT_LEVEL = Qxp.DEFAULT_LEVEL;

    /** The highest value level; quicxd never evicts a task of this level. */
    public static final int CRITICAL_LEVEL = Qxp.MAX_LEVEL;

    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final long responseTimeoutMs;
    private final int maxBody;

    private final Object writeLock = new Object();   // one writer at a time; keeps sends in queue order
    private final Object stateLock = new Object();   // guards everything below

    private Socket socket;
    private OutputStream out;
    private final ArrayDeque<Pending> awaitingAnswer = new ArrayDeque<>();
    private final Map<Integer, CompletableFuture<TaskResult>> awaitingResult = new HashMap<>();
    private boolean closed;

    /** A client for quicxd at {@code host:port}, with default timeouts. */
    public QuicxClient(String host, int port) {
        this(builder().host(host).port(port));
    }

    private QuicxClient(Builder b) {
        this.host = b.host;
        this.port = b.port;
        this.connectTimeoutMs = (int) Math.min(Integer.MAX_VALUE, b.connectTimeout.toMillis());
        this.responseTimeoutMs = b.responseTimeout.toMillis();
        this.maxBody = b.maxBody;
    }

    /** Starts a builder for a client with non-default settings. */
    public static Builder builder() {
        return new Builder();
    }

    // ---- submitting --------------------------------------------------------

    /**
     * Submits a task and returns the id quicxd gave it, once quicxd has
     * queued it. Does not wait for a worker.
     *
     * @param taskType 1 to 31 bytes of UTF-8, the type workers register for
     * @param payload  the task's data; with the type it must fit in 1,024 bytes
     * @return the task id
     * @throws QuicxRejectedException   if quicxd refused the task, e.g. because its memory budget is full
     * @throws QuicxConnectionException if quicxd could not be reached or did not answer in time
     */
    public int submit(String taskType, byte[] payload) {
        Pending p = send(taskType, payload, -1, false);
        return await(p);
    }

    /**
     * Submits a task with a value level and returns its id once quicxd has
     * queued it. Higher levels are handed to workers first. When quicxd's
     * memory budget is full, a task can push out a waiting task of a lower
     * level, whose producer then gets a result with
     * {@link TaskResult#isEvicted()}; level {@value #CRITICAL_LEVEL} is never
     * evicted. Needs quicxd 2.1.0 or later; an older quicxd refuses it with
     * {@link ErrorCode#INVALID_MESSAGE}.
     *
     * @param level 0 (least valuable) to 15 (critical); a plain {@link #submit(String, byte[])} is level {@value #DEFAULT_LEVEL}
     * @throws IllegalArgumentException if the level is outside 0-15
     */
    public int submit(String taskType, byte[] payload, int level) {
        Pending p = send(taskType, payload, Qxp.checkLevel(level), false);
        return await(p);
    }

    /** {@link #submit(String, byte[], int)} with the payload encoded as UTF-8. */
    public int submit(String taskType, String payload, int level) {
        return submit(taskType, utf8(payload), level);
    }

    /** {@link #submit(String, byte[])} with the payload encoded as UTF-8. */
    public int submit(String taskType, String payload) {
        return submit(taskType, utf8(payload));
    }

    /**
     * Submits a task and returns a future that completes when a worker
     * reports it DONE or FAILED. The future fails with
     * {@link QuicxRejectedException} if quicxd refuses the task and with
     * {@link QuicxConnectionException} if the connection breaks before the
     * result arrives.
     */
    public CompletableFuture<TaskResult> submitAsync(String taskType, byte[] payload) {
        return send(taskType, payload, -1, true).result;
    }

    /**
     * {@link #submitAsync(String, byte[])} with a value level (see
     * {@link #submit(String, byte[], int)}). If quicxd evicts the task to
     * make room for a more valuable one, the future completes with a result
     * for which {@link TaskResult#isEvicted()} is true.
     */
    public CompletableFuture<TaskResult> submitAsync(String taskType, byte[] payload, int level) {
        return send(taskType, payload, Qxp.checkLevel(level), true).result;
    }

    /** {@link #submitAsync(String, byte[], int)} with the payload encoded as UTF-8. */
    public CompletableFuture<TaskResult> submitAsync(String taskType, String payload, int level) {
        return submitAsync(taskType, utf8(payload), level);
    }

    /** {@link #submitAsync(String, byte[])} with the payload encoded as UTF-8. */
    public CompletableFuture<TaskResult> submitAsync(String taskType, String payload) {
        return submitAsync(taskType, utf8(payload));
    }

    /**
     * Submits a task and blocks until a worker reports it DONE or FAILED.
     *
     * @param timeout how long to wait for the result
     * @throws QuicxException if no result arrives within {@code timeout}
     */
    public TaskResult submitAndWait(String taskType, byte[] payload, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");   // before anything is sent
        return waitFor(submitAsync(taskType, payload), timeout);
    }

    /** {@link #submitAndWait(String, byte[], Duration)} with a value level (see {@link #submit(String, byte[], int)}). */
    public TaskResult submitAndWait(String taskType, byte[] payload, int level, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return waitFor(submitAsync(taskType, payload, level), timeout);
    }

    /** {@link #submitAndWait(String, byte[], int, Duration)} with the payload encoded as UTF-8. */
    public TaskResult submitAndWait(String taskType, String payload, int level, Duration timeout) {
        return submitAndWait(taskType, utf8(payload), level, timeout);
    }

    private static TaskResult waitFor(CompletableFuture<TaskResult> f, Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        try {
            return f.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new QuicxException("no result within " + timeout.toMillis() + " ms");
        } catch (ExecutionException e) {
            throw unwrap(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QuicxException("interrupted while waiting for the result", e);
        }
    }

    /** {@link #submitAndWait(String, byte[], Duration)} with the payload encoded as UTF-8. */
    public TaskResult submitAndWait(String taskType, String payload, Duration timeout) {
        return submitAndWait(taskType, utf8(payload), timeout);
    }

    /** The host this client connects to. */
    public String host() {
        return host;
    }

    /** The port this client connects to. */
    public int port() {
        return port;
    }

    /** The largest QXP body this client sends: 1,024 bytes unless set with {@link Builder#maxBody(int)}. */
    public int maxBody() {
        return maxBody;
    }

    /**
     * Closes the connection. Submits that are still waiting fail with
     * {@link QuicxConnectionException}. Calling it twice does nothing.
     */
    @Override
    public void close() {
        synchronized (stateLock) {
            closed = true;
        }
        drop(null, "the client was closed");
    }

    // ---- sending -----------------------------------------------------------

    private static final class Pending {
        final boolean wantResult;
        final CompletableFuture<Integer> accepted = new CompletableFuture<>();
        final CompletableFuture<TaskResult> result = new CompletableFuture<>();

        Pending(boolean wantResult) {
            this.wantResult = wantResult;
        }

        void fail(Throwable t) {
            accepted.completeExceptionally(t);
            result.completeExceptionally(t);
        }
    }

    /** level -1 sends a plain SUBMIT, 0-15 a SUBMIT_VALUED. */
    private Pending send(String taskType, byte[] payload, int level, boolean wantResult) {
        // checks the type, level and size before anything is sent
        byte[] body = level < 0 ? Qxp.submitBody(taskType, payload, maxBody)
                                : Qxp.valuedSubmitBody(level, taskType, payload, maxBody);
        byte frameType = level < 0 ? Qxp.SUBMIT : Qxp.SUBMIT_VALUED;
        Pending p = new Pending(wantResult);
        synchronized (writeLock) {
            OutputStream o = connection();
            synchronized (stateLock) {
                awaitingAnswer.addLast(p);
            }
            try {
                Qxp.writeFrame(o, frameType, body);
            } catch (IOException e) {
                drop(e, "sending to quicxd at " + host + ":" + port + " failed");
            }
        }
        return p;
    }

    private int await(Pending p) {
        try {
            return p.accepted.get(responseTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // the answers on this connection can no longer be matched to requests
            drop(e, "quicxd did not answer within " + responseTimeoutMs + " ms");
            throw new QuicxConnectionException("quicxd at " + host + ":" + port + " did not answer within "
                    + responseTimeoutMs + " ms; the task may or may not have been queued", e);
        } catch (ExecutionException e) {
            throw unwrap(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new QuicxException("interrupted while waiting for quicxd", e);
        }
    }

    /** The open connection, opening one if needed. Called with writeLock held. */
    private OutputStream connection() {
        synchronized (stateLock) {
            if (closed) throw new QuicxException("the client is closed");
            if (socket != null) return out;
        }
        Socket s = new Socket();
        InputStream in;
        OutputStream o;
        try {
            s.setTcpNoDelay(true);
            s.setKeepAlive(true);
            s.connect(new InetSocketAddress(host, port), connectTimeoutMs);
            in = new BufferedInputStream(s.getInputStream());
            o = new BufferedOutputStream(s.getOutputStream());
        } catch (IOException e) {
            closeQuietly(s);
            throw new QuicxConnectionException("cannot connect to quicxd at " + host + ":" + port, e);
        }
        synchronized (stateLock) {
            if (closed) {
                closeQuietly(s);
                throw new QuicxException("the client is closed");
            }
            socket = s;
            out = o;
        }
        Thread reader = new Thread(() -> readLoop(s, in), "quicx-client " + host + ":" + port);
        reader.setDaemon(true);
        reader.start();
        return o;
    }

    // ---- receiving ---------------------------------------------------------

    private void readLoop(Socket s, InputStream in) {
        try {
            while (true) {
                Frame f = Qxp.readFrame(in);
                switch (f.type) {
                    case Qxp.OK:     onOk(Qxp.getInt(f.body, 0)); break;
                    case Qxp.ERROR:  onError(f.body); break;
                    case Qxp.DONE:   onResult(Qxp.getInt(f.body, 0), null); break;
                    case Qxp.FAILED: onResult(Qxp.getInt(f.body, 0), Qxp.utf8(f.body, 4, f.body.length - 4)); break;
                    default:         break;   // PONG, WAIT and anything newer: nothing to do
                }
            }
        } catch (IOException | RuntimeException e) {
            synchronized (stateLock) {
                if (socket != s) return;   // already replaced or closed
            }
            drop(e, "the connection to quicxd at " + host + ":" + port + " was lost");
        }
    }

    private void onOk(int taskId) {
        Pending p;
        synchronized (stateLock) {
            p = awaitingAnswer.pollFirst();
            if (p == null) return;
            // DONE for this id can only arrive after this OK, and this thread reads both in order
            if (p.wantResult) awaitingResult.put(taskId, p.result);
        }
        p.accepted.complete(taskId);
    }

    private void onError(byte[] body) {
        ErrorCode code = body.length > 0 ? ErrorCode.of(body[0] & 0xFF) : ErrorCode.UNKNOWN;
        String reason = body.length > 1 ? Qxp.utf8(body, 1, body.length - 1) : "";
        Pending p;
        synchronized (stateLock) {
            p = awaitingAnswer.pollFirst();
        }
        if (p != null) p.fail(new QuicxRejectedException(code, reason));
    }

    private void onResult(int taskId, String failureReason) {
        CompletableFuture<TaskResult> f;
        synchronized (stateLock) {
            f = awaitingResult.remove(taskId);
        }
        if (f != null) f.complete(failureReason == null ? TaskResult.done(taskId) : TaskResult.failed(taskId, failureReason));
    }

    /** Closes the current connection and fails everything still waiting on it. */
    private void drop(Throwable cause, String why) {
        Socket s;
        List<Pending> pending;
        List<CompletableFuture<TaskResult>> results;
        synchronized (stateLock) {
            s = socket;
            socket = null;
            out = null;
            pending = new ArrayList<>(awaitingAnswer);
            awaitingAnswer.clear();
            results = new ArrayList<>(awaitingResult.values());
            awaitingResult.clear();
        }
        closeQuietly(s);
        if (pending.isEmpty() && results.isEmpty()) return;
        QuicxConnectionException e = new QuicxConnectionException(why, cause);
        for (Pending p : pending) p.fail(e);
        for (CompletableFuture<TaskResult> r : results) r.completeExceptionally(e);
    }

    // ---- helpers -----------------------------------------------------------

    private static QuicxException unwrap(ExecutionException e) {
        Throwable c = e.getCause();
        if (c instanceof QuicxException) return (QuicxException) c;
        return new QuicxException(String.valueOf(c), c);
    }

    private static byte[] utf8(String s) {
        return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
    }

    private static void closeQuietly(Socket s) {
        if (s == null) return;
        try {
            s.close();
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }

    /** Settings for a {@link QuicxClient}. */
    public static final class Builder {
        private String host = "localhost";
        private int port = DEFAULT_PORT;
        private Duration connectTimeout = Duration.ofSeconds(5);
        private Duration responseTimeout = Duration.ofSeconds(10);
        private int maxBody = Qxp.MAX_BODY;

        private Builder() {}

        /** Host name or address of quicxd. Default {@code localhost}. */
        public Builder host(String host) {
            this.host = Objects.requireNonNull(host, "host");
            return this;
        }

        /** Port of quicxd. Default {@value QuicxClient#DEFAULT_PORT}. */
        public Builder port(int port) {
            if (port < 1 || port > 65535) throw new IllegalArgumentException("port must be 1-65535, not " + port);
            this.port = port;
            return this;
        }

        /** How long opening the connection may take. Default 5 seconds. */
        public Builder connectTimeout(Duration timeout) {
            this.connectTimeout = positive(timeout, "connectTimeout");
            return this;
        }

        /** How long {@link QuicxClient#submit} waits for quicxd to accept a task. Default 10 seconds. */
        public Builder responseTimeout(Duration timeout) {
            this.responseTimeout = positive(timeout, "responseTimeout");
            return this;
        }

        /**
         * The largest QXP body quicxd accepts: 1,024 bytes by default, or N for a
         * daemon built with {@code make MAX_PAYLOAD=N}. The client refuses larger
         * tasks before sending them. At most {@value Qxp#MAX_BODY_LIMIT}.
         */
        public Builder maxBody(int bytes) {
            if (bytes < 16 || bytes > Qxp.MAX_BODY_LIMIT)
                throw new IllegalArgumentException("maxBody must be 16-" + Qxp.MAX_BODY_LIMIT + ", not " + bytes);
            this.maxBody = bytes;
            return this;
        }

        /** Creates the client. It connects on the first submit. */
        public QuicxClient build() {
            return new QuicxClient(this);
        }

        static Duration positive(Duration d, String name) {
            Objects.requireNonNull(d, name);
            if (d.isNegative() || d.isZero()) throw new IllegalArgumentException(name + " must be positive");
            return d;
        }
    }
}
