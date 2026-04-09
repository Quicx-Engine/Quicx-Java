package dev.quicx;

import dev.quicx.internal.Connection;
import dev.quicx.internal.Protocol;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class QuicxWorker implements AutoCloseable {

    public interface TaskHandler {
        void handle(byte[] payload) throws Exception;
    }

    private final Connection conn;
    private final Map<String, TaskHandler> handlers =
        new HashMap<>();
    private final AtomicBoolean running =
        new AtomicBoolean(false);

    public QuicxWorker(String host, int port) {
        this.conn = new Connection(host, port);
    }

    // register handler for a task type
    public QuicxWorker handle(String taskType,
                              TaskHandler handler) {
        handlers.put(taskType, handler);
        return this;
    }

    // connect and process tasks — blocks forever
    public void start() throws Exception {
        running.set(true);

        while (running.get()) {
            try {
                conn.connect();
                System.out.println("quicx worker connected to "
                    + conn.getHost() + ":" + conn.getPort());

                conn.protocol().sendReady();
                processLoop();

            } catch (Exception e) {
                System.err.println("worker disconnected: "
                    + e.getMessage());
                conn.close();

                if (running.get()) {
                    System.err.println("reconnecting in 3s...");
                    Thread.sleep(3000);
                }
            }
        }
    }

    private void processLoop() throws Exception {
        while (running.get()) {
            Protocol.Message msg =
                conn.protocol().readMessage();

            switch (msg.type) {
                case Protocol.MSG_TASK:
                    handleTask(msg);
                    break;

                case Protocol.MSG_PONG:
                    // heartbeat response — alive
                    break;

                case Protocol.MSG_WAIT:
                    // no tasks yet — keep waiting
                    break;

                default:
                    System.err.println(
                        "unexpected message type: " + msg.type);
            }
        }
    }

    private void handleTask(Protocol.Message msg) {
        TaskHandler handler = handlers.get(msg.taskType);

        if (handler == null) {
            System.err.println(
                "no handler for task type: " + msg.taskType);
            try {
                conn.protocol().sendFailed(msg.taskId,
                    "no handler for: " + msg.taskType);
            } catch (Exception e) {
                System.err.println("failed to send FAILED: "
                    + e.getMessage());
            }
            return;
        }

        try {
            System.out.println("executing task " + msg.taskId
                + " type=" + msg.taskType);
            handler.handle(msg.payload);
            conn.protocol().sendDone(msg.taskId);
            System.out.println("task done: " + msg.taskId);

        } catch (Exception e) {
            System.err.println("task failed: " + msg.taskId
                + " error=" + e.getMessage());
            try {
                conn.protocol().sendFailed(
                    msg.taskId, e.getMessage());
            } catch (Exception ex) {
                System.err.println("failed to send FAILED: "
                    + ex.getMessage());
            }
        }
    }

    @Override
    public void close() throws Exception {
        running.set(false);
        conn.close();
    }
}