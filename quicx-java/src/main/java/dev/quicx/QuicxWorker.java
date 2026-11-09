package dev.quicx;

import dev.quicx.internal.Frame;
import dev.quicx.internal.Qxp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Takes tasks from quicxd and runs them, one at a time.
 *
 * <pre>{@code
 * QuicxWorker worker = new QuicxWorker("localhost", 16381)
 *     .handle("send_email", payload -> mailer.send(new String(payload, UTF_8)))
 *     .handleTask("resize", task -> images.resize(task.id(), task.payload()));
 *
 * worker.start();   // blocks; call worker.stop() from another thread to end it
 * }</pre>
 *
 * <p>After a handler returns, the worker reports DONE; if it throws, the
 * worker reports FAILED with the exception's message (QXP carries at most
 * 63 bytes of it). Both are passed on to the producer. A task whose type has
 * no handler is reported FAILED, unless a {@link #fallback fallback} is set.
 *
 * <p>The worker keeps itself connected: if quicxd goes away it reconnects,
 * waiting a little longer after each failed attempt (0.5 s up to 30 s by
 * default). While idle it sends a heartbeat every few seconds, so a
 * connection that died silently is noticed. It logs through
 * {@link System.Logger} under the name {@code dev.quicx.QuicxWorker}.
 */
public final class QuicxWorker implements AutoCloseable {

    /** Runs the payload of one task. Return to report DONE, throw to report FAILED. */
    @FunctionalInterface
    public interface TaskHandler {
        void handle(byte[] payload) throws Exception;
    }

    /** Runs one task, with its id and type. Return to report DONE, throw to report FAILED. */
    @FunctionalInterface
    public interface TaskProcessor {
        void process(Task task) throws Exception;
    }

    private static final Logger LOG = System.getLogger(QuicxWorker.class.getName());

    private final String host;
    private final int port;
    private final int connectTimeoutMs;
    private final long heartbeatMs;
    private final long minBackoffMs;
    private final long maxBackoffMs;

    private final ConcurrentHashMap<String, TaskProcessor> processors = new ConcurrentHashMap<>();
    private volatile TaskProcessor fallback;

    private final AtomicBoolean started = new AtomicBoolean();
    private volatile boolean running;
    private final Object lock = new Object();   // guards socket, out, busy
    private Socket socket;
    private OutputStream out;
    private boolean busy;
    private volatile long lastWriteNanos;

    private final AtomicLong done = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();

    /** A worker for quicxd at {@code host:port}, with default settings. */
    public QuicxWorker(String host, int port) {
        this(builder().host(host).port(port));
    }

    private QuicxWorker(Builder b) {
        this.host = b.host;
        this.port = b.port;
        this.connectTimeoutMs = (int) Math.min(Integer.MAX_VALUE, b.connectTimeout.toMillis());
        this.heartbeatMs = b.heartbeat.toMillis();
        this.minBackoffMs = b.minBackoff.toMillis();
        this.maxBackoffMs = b.maxBackoff.toMillis();
    }

    /** Starts a builder for a worker with non-default settings. */
    public static Builder builder() {
        return new Builder();
    }

    // ---- handlers ----------------------------------------------------------

    /**
     * Runs {@code handler} for every task of {@code taskType}. Registering a
     * type again replaces its handler.
     *
     * @throws IllegalArgumentException if the type is not 1 to 31 bytes of UTF-8
     */
    public QuicxWorker handle(String taskType, TaskHandler handler) {
        Objects.requireNonNull(handler, "handler");
        return handleTask(taskType, task -> handler.handle(task.payloadNoCopy()));
    }

    /** Like {@link #handle(String, TaskHandler)}, for handlers that need the task's id or type. */
    public QuicxWorker handleTask(String taskType, TaskProcessor processor) {
        Qxp.typeBytes(taskType);
        processors.put(taskType, Objects.requireNonNull(processor, "processor"));
        return this;
    }

    /** Runs tasks whose type has no handler. Without one, such tasks are reported FAILED. */
    public QuicxWorker fallback(TaskProcessor processor) {
        this.fallback = processor;
        return this;
    }

    // ---- running -----------------------------------------------------------

    /**
     * Connects, registers with READY and runs tasks until {@link #stop()} is
     * called. Reconnects by itself when the connection is lost. Blocks the
     * calling thread.
     *
     * @throws IllegalStateException if the worker was already started
     */
    public void start() {
        if (!started.compareAndSet(false, true)) throw new IllegalStateException("the worker was already started");
        running = true;
        long backoff = minBackoffMs;
        while (running) {
            Socket s = new Socket();
            Thread heartbeat = null;
            try {
                InputStream in = connect(s);
                heartbeat = startHeartbeat(s);
                LOG.log(Level.INFO, "connected to quicxd at {0}:{1}, handling {2}", host, String.valueOf(port), types());
                backoff = minBackoffMs;
                serve(in);
            } catch (QuicxRejectedException e) {
                if (running) LOG.log(Level.WARNING, "quicxd refused this worker: {0}", e.reason());
            } catch (IOException | RuntimeException e) {
                if (running) LOG.log(Level.WARNING, "connection to quicxd at {0}:{1} lost: {2}", host, String.valueOf(port), e.toString());
            } finally {
                if (heartbeat != null) heartbeat.interrupt();
                synchronized (lock) {
                    if (socket == s) {
                        socket = null;
                        out = null;
                    }
                }
                closeQuietly(s);
            }
            if (!running) break;
            long wait = backoff / 2 + ThreadLocalRandom.current().nextLong(backoff / 2 + 1);
            LOG.log(Level.INFO, "reconnecting in {0} ms", String.valueOf(wait));
            pause(wait);
            backoff = Math.min(maxBackoffMs, backoff * 2);
        }
        LOG.log(Level.INFO, "worker stopped after {0} done and {1} failed tasks",
                String.valueOf(done.get()), String.valueOf(failed.get()));
    }

    /** Runs {@link #start()} on a new daemon thread and returns that thread. */
    public Thread startInBackground() {
        Thread t = new Thread(this::start, "quicx-worker " + host + ":" + port);
        t.setDaemon(true);
        t.start();
        return t;
    }

    /**
     * Stops the worker. A task that is running finishes and is reported
     * first; then the connection is closed and {@link #start()} returns.
     */
    public void stop() {
        synchronized (lock) {
            running = false;
            if (!busy) closeQuietly(socket);
            lock.notifyAll();   // ends a wait between reconnect attempts
        }
    }

    /** Same as {@link #stop()}. */
    @Override
    public void close() {
        stop();
    }

    /** Whether {@link #start()} is running and has not been stopped. */
    public boolean isRunning() {
        return running;
    }

    /** Tasks reported DONE since the worker started. */
    public long tasksDone() {
        return done.get();
    }

    /** Tasks reported FAILED since the worker started. */
    public long tasksFailed() {
        return failed.get();
    }

    // ---- the connection ----------------------------------------------------

    private InputStream connect(Socket s) throws IOException {
        s.setTcpNoDelay(true);
        s.setKeepAlive(true);
        s.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        // with heartbeats every heartbeatMs, three quiet intervals mean the connection is dead
        s.setSoTimeout((int) Math.min(Integer.MAX_VALUE, heartbeatMs * 3));
        InputStream in = new BufferedInputStream(s.getInputStream());
        synchronized (lock) {
            if (!running) throw new IOException("stopped");
            socket = s;
            out = new BufferedOutputStream(s.getOutputStream());
        }
        write(Qxp.READY, null);
        return in;
    }

    private void serve(InputStream in) throws IOException {
        while (running) {
            Frame f;
            try {
                f = Qxp.readFrame(in);
            } catch (SocketTimeoutException e) {
                throw new IOException("no answer to heartbeats for " + heartbeatMs * 3 + " ms", e);
            }
            switch (f.type) {
                case Qxp.TASK:
                    run(decodeTask(f.body));
                    break;
                case Qxp.ERROR: {
                    ErrorCode code = f.body.length > 0 ? ErrorCode.of(f.body[0] & 0xFF) : ErrorCode.UNKNOWN;
                    String reason = f.body.length > 1 ? Qxp.utf8(f.body, 1, f.body.length - 1) : "";
                    throw new QuicxRejectedException(code, reason);
                }
                default:
                    break;   // PONG, WAIT, a stray DONE/FAILED: nothing to do
            }
        }
    }

    private static Task decodeTask(byte[] body) throws IOException {
        if (body.length < 5) throw new IOException("TASK frame of " + body.length + " bytes");
        int id = Qxp.getInt(body, 0);
        int typeLen = body[4] & 0xFF;
        if (5 + typeLen > body.length) throw new IOException("TASK frame with a broken type length");
        String type = Qxp.utf8(body, 5, typeLen);
        byte[] payload = new byte[body.length - 5 - typeLen];
        System.arraycopy(body, 5 + typeLen, payload, 0, payload.length);
        return new Task(id, type, payload);
    }

    private void run(Task task) throws IOException {
        TaskProcessor p = processors.get(task.type());
        if (p == null) p = fallback;
        synchronized (lock) {
            busy = true;
        }
        try {
            if (p == null) {
                failed.incrementAndGet();
                LOG.log(Level.WARNING, "no handler for task type {0} (task {1})", task.type(), String.valueOf(task.id()));
                write(Qxp.FAILED, Qxp.failedBody(task.id(), "no handler for task type " + task.type()));
                return;
            }
            Throwable error = null;
            try {
                p.process(task);
            } catch (Throwable t) {
                error = t;
            }
            if (error == null) {
                done.incrementAndGet();
                write(Qxp.DONE, Qxp.doneBody(task.id()));
            } else {
                failed.incrementAndGet();
                LOG.log(Level.WARNING, "task " + task.id() + " (" + task.type() + ") failed", error);
                write(Qxp.FAILED, Qxp.failedBody(task.id(), describe(error)));
                if (error instanceof VirtualMachineError) throw (VirtualMachineError) error;
            }
        } finally {
            synchronized (lock) {
                busy = false;
                if (!running) closeQuietly(socket);
            }
        }
    }

    private void write(byte type, byte[] body) throws IOException {
        OutputStream o;
        synchronized (lock) {
            o = out;
        }
        if (o == null) throw new IOException("not connected");
        synchronized (o) {
            Qxp.writeFrame(o, type, body);
        }
        lastWriteNanos = System.nanoTime();
    }

    private Thread startHeartbeat(Socket s) {
        Thread t = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted() && !s.isClosed()) {
                    Thread.sleep(heartbeatMs);
                    if (System.nanoTime() - lastWriteNanos >= heartbeatMs * 1_000_000L) write(Qxp.HEARTBEAT, null);
                }
            } catch (InterruptedException | IOException e) {
                // the reading thread notices a dead connection on its own
            }
        }, "quicx-worker-heartbeat " + host + ":" + port);
        t.setDaemon(true);
        t.start();
        return t;
    }

    // ---- helpers -----------------------------------------------------------

    /** Waits up to {@code ms}, or until {@link #stop()}. */
    private void pause(long ms) {
        long deadline = System.nanoTime() + ms * 1_000_000L;
        synchronized (lock) {
            long left;
            while (running && (left = (deadline - System.nanoTime()) / 1_000_000L) > 0) {
                try {
                    lock.wait(left);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }
        }
    }

    private Set<String> types() {
        return new TreeSet<>(processors.keySet());
    }

    private static String describe(Throwable t) {
        String m = t.getMessage();
        String name = t.getClass().getSimpleName();
        return m == null || m.isEmpty() ? name : name + ": " + m;
    }

    private static void closeQuietly(Socket s) {
        if (s == null) return;
        try {
            s.close();
        } catch (IOException ignored) {
            // nothing useful to do
        }
    }

    /** Settings for a {@link QuicxWorker}. */
    public static final class Builder {
        private String host = "localhost";
        private int port = QuicxClient.DEFAULT_PORT;
        private Duration connectTimeout = Duration.ofSeconds(5);
        private Duration heartbeat = Duration.ofSeconds(5);
        private Duration minBackoff = Duration.ofMillis(500);
        private Duration maxBackoff = Duration.ofSeconds(30);

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
            this.connectTimeout = QuicxClient.Builder.positive(timeout, "connectTimeout");
            return this;
        }

        /**
         * How often an idle worker sends a heartbeat. If nothing comes back for
         * three intervals, the connection counts as dead. Default 5 seconds.
         */
        public Builder heartbeatInterval(Duration interval) {
            this.heartbeat = QuicxClient.Builder.positive(interval, "heartbeatInterval");
            return this;
        }

        /** The shortest and longest wait between reconnect attempts. Default 0.5 s and 30 s. */
        public Builder reconnectDelay(Duration min, Duration max) {
            Duration lo = QuicxClient.Builder.positive(min, "min");
            Duration hi = QuicxClient.Builder.positive(max, "max");
            if (hi.compareTo(lo) < 0) throw new IllegalArgumentException("max reconnect delay is below the min");
            this.minBackoff = lo;
            this.maxBackoff = hi;
            return this;
        }

        /** Creates the worker. Register handlers, then call {@link QuicxWorker#start()}. */
        public QuicxWorker build() {
            return new QuicxWorker(this);
        }
    }
}
