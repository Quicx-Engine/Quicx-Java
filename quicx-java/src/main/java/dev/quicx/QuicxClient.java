package dev.quicx;

import dev.quicx.internal.Connection;
import dev.quicx.internal.Protocol;

public class QuicxClient implements AutoCloseable {

    private final Connection conn;

    public QuicxClient(String host, int port) {
        this.conn = new Connection(host, port);
    }

    // submit a task — returns task ID
    public int submit(String taskType,
                      byte[] payload) throws Exception {
        conn.connect();
        try {
            conn.protocol().sendSubmit(taskType, payload);

            Protocol.Message response =
                conn.protocol().readMessage();

            if (response.type == Protocol.MSG_OK)
                return response.taskId;

            if (response.type == Protocol.MSG_ERROR)
                throw new QuicxException(
                    "task rejected: " + response.errorMsg);

            throw new QuicxException(
                "unexpected response: " + response.type);
        } finally {
            conn.close();  // one connection per submit — stateless
        }
    }

    // convenience — string payload
    public int submit(String taskType,
                      String payload) throws Exception {
        return submit(taskType,
                      payload.getBytes("UTF-8"));
    }

    @Override
    public void close() {
        conn.close();
    }
}