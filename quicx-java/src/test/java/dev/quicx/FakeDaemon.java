package dev.quicx;

import dev.quicx.internal.Frame;
import dev.quicx.internal.Qxp;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** A scripted stand-in for quicxd: every accepted connection runs the next script. */
final class FakeDaemon implements AutoCloseable {

    interface Script {
        void run(Peer peer) throws Exception;
    }

    private final ServerSocket server;
    private final LinkedBlockingQueue<Script> scripts = new LinkedBlockingQueue<>();
    final List<Peer> peers = new CopyOnWriteArrayList<>();
    final LinkedBlockingQueue<Throwable> errors = new LinkedBlockingQueue<>();

    /** Every connection runs the scripts in order; the last one repeats. */
    FakeDaemon(Script... perConnection) throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        for (Script s : perConnection) scripts.add(s);
        Script last = perConnection[perConnection.length - 1];
        Thread acceptor = new Thread(() -> {
            while (!server.isClosed()) {
                try {
                    Socket s = server.accept();
                    Peer p = new Peer(s);
                    peers.add(p);
                    Script script = scripts.isEmpty() ? last : scripts.poll();
                    Thread t = new Thread(() -> {
                        try {
                            script.run(p);
                        } catch (Throwable e) {
                            if (!p.closed) errors.add(e);
                        }
                    }, "fake-daemon-peer");
                    t.setDaemon(true);
                    t.start();
                } catch (IOException e) {
                    return;
                }
            }
        }, "fake-daemon");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    int port() {
        return server.getLocalPort();
    }

    @Override
    public void close() throws IOException {
        server.close();
        for (Peer p : peers) p.close();
    }

    static final class Peer {
        final Socket socket;
        final InputStream in;
        final OutputStream out;
        volatile boolean closed;

        Peer(Socket s) throws IOException {
            socket = s;
            in = new BufferedInputStream(s.getInputStream());
            out = s.getOutputStream();
        }

        Frame read() throws IOException {
            return Qxp.readFrame(in);
        }

        /** Reads frames until one of {@code type} arrives, skipping heartbeats. */
        Frame expect(byte type) throws IOException {
            while (true) {
                Frame f = read();
                if (f.type == type) return f;
                if (f.type != Qxp.HEARTBEAT) throw new AssertionError("expected type " + type + ", got " + f.type);
            }
        }

        void send(byte type, byte[] body) throws IOException {
            Qxp.writeFrame(out, type, body);
        }

        void ok(int id) throws IOException {
            send(Qxp.OK, Qxp.doneBody(id));
        }

        void done(int id) throws IOException {
            send(Qxp.DONE, Qxp.doneBody(id));
        }

        void failed(int id, String reason) throws IOException {
            send(Qxp.FAILED, Qxp.failedBody(id, reason));
        }

        void error(int code, String reason) throws IOException {
            byte[] r = reason.getBytes(StandardCharsets.UTF_8);
            byte[] body = new byte[1 + r.length];
            body[0] = (byte) code;
            System.arraycopy(r, 0, body, 1, r.length);
            send(Qxp.ERROR, body);
        }

        void task(int id, String type, byte[] payload) throws IOException {
            byte[] t = type.getBytes(StandardCharsets.UTF_8);
            byte[] body = new byte[5 + t.length + payload.length];
            body[0] = (byte) (id >>> 24);
            body[1] = (byte) (id >>> 16);
            body[2] = (byte) (id >>> 8);
            body[3] = (byte) id;
            body[4] = (byte) t.length;
            System.arraycopy(t, 0, body, 5, t.length);
            System.arraycopy(payload, 0, body, 5 + t.length, payload.length);
            send(Qxp.TASK, body);
        }

        /** Blocks until the client closes its side. */
        void awaitClose() throws IOException {
            try {
                while (true) read();
            } catch (IOException e) {
                // closed
            }
        }

        void close() {
            closed = true;
            try {
                socket.close();
            } catch (IOException ignored) {
                // fine
            }
        }
    }

    static void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
