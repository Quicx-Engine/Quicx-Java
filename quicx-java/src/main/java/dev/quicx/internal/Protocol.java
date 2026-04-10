package dev.quicx.internal;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Protocol {

    public static final byte VERSION       = 1;
    public static final byte MSG_SUBMIT    = 0x01;
    public static final byte MSG_OK        = 0x02;
    public static final byte MSG_ERROR     = 0x03;
    public static final byte MSG_READY     = 0x04;
    public static final byte MSG_DONE      = 0x05;
    public static final byte MSG_FAILED    = 0x06;
    public static final byte MSG_TASK      = 0x07;
    public static final byte MSG_WAIT      = 0x08;
    public static final byte MSG_HEARTBEAT = 0x09;
    public static final byte MSG_PONG      = 0x0A;
    public static final byte MSG_STATS     = 0x0B;
    public static final byte MSG_STATS_RESPONSE = 0x0C;

    private final DataInputStream  in;
    private final DataOutputStream out;

    public Protocol(Socket socket) throws IOException {
        // DataInputStream reads in network byte order (big endian)
        this.in  = new DataInputStream(
                       new BufferedInputStream(
                           socket.getInputStream()));
        this.out = new DataOutputStream(
                       new BufferedOutputStream(
                           socket.getOutputStream()));
        socket.setTcpNoDelay(true);  // no Nagle — send immediately
    }

    // send ──────────────────────────────────────────────────────────────

    // send a frame — [version:1][type:1][length:4][payload]
    private void sendFrame(byte type, byte[] payload) throws IOException {
        int len = payload != null ? payload.length : 0;
        out.writeByte(VERSION);
        out.writeByte(type);
        out.writeInt(len);
        if (payload != null && len > 0)
            out.write(payload);
        out.flush();
    }

    // MSG_SUBMIT — [type_len:1][type:bytes][payload:rest]
    public void sendSubmit(String taskType,
                           byte[] payload) throws IOException {
        byte[] typeBytes = taskType.getBytes("UTF-8");
        int    total     = 1 + typeBytes.length +
                           (payload != null ? payload.length : 0);

        ByteBuffer buf = ByteBuffer.allocate(total);
        buf.put((byte)typeBytes.length);
        buf.put(typeBytes);
        if (payload != null)
            buf.put(payload);

        sendFrame(MSG_SUBMIT, buf.array());
    }

    // MSG_READY — no payload
    public void sendReady() throws IOException {
        sendFrame(MSG_READY, null);
    }

    // MSG_DONE — [task_id:4]
    public void sendDone(int taskId) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4)
                                   .order(ByteOrder.BIG_ENDIAN);
        buf.putInt(taskId);
        sendFrame(MSG_DONE, buf.array());
    }

    // MSG_FAILED — [task_id:4][reason:rest]
    public void sendFailed(int taskId,
                           String reason) throws IOException {
        byte[] reasonBytes = reason.getBytes("UTF-8");
        ByteBuffer buf = ByteBuffer.allocate(4 + reasonBytes.length);
        buf.order(ByteOrder.BIG_ENDIAN);
        buf.putInt(taskId);
        buf.put(reasonBytes);
        sendFrame(MSG_FAILED, buf.array());
    }

    // MSG_HEARTBEAT — no payload
    public void sendHeartbeat() throws IOException {
        sendFrame(MSG_HEARTBEAT, null);
    }

    // receive ───────────────────────────────────────────────────────────

    public static class Message {
        public byte   type;
        public int    taskId;
        public String taskType;
        public byte[] payload;
        public String errorMsg;
    }

    private void readExact(byte[] buf,
                           int offset,
                           int len) throws IOException {
        int total = 0;
        while (total < len) {
            int r = in.read(buf, offset + total, len - total);
            if (r < 0) throw new EOFException("connection closed");
            total += r;
        }
    }

    public Message readMessage() throws IOException {
        // read 6 byte header
        byte version = in.readByte();
        byte type    = in.readByte();
        int  length  = in.readInt();

        if (version != VERSION)
            throw new IOException("bad protocol version: " + version);

        byte[] payload = null;
        if (length > 0) {
            payload = new byte[length];
            readExact(payload, 0, length);
        }

        Message msg  = new Message();
        msg.type     = type;

        if (type == MSG_OK && payload != null) {
            msg.taskId = ByteBuffer.wrap(payload)
                                   .order(ByteOrder.BIG_ENDIAN)
                                   .getInt();

        } else if (type == MSG_TASK && payload != null) {
            ByteBuffer buf = ByteBuffer.wrap(payload)
                                       .order(ByteOrder.BIG_ENDIAN);
            msg.taskId = buf.getInt();

            int typeLen = buf.get() & 0xFF;
            byte[] typeBytes = new byte[typeLen];
            buf.get(typeBytes);
            msg.taskType = new String(typeBytes, "UTF-8");

            int payloadLen = payload.length - 5 - typeLen;
            if (payloadLen > 0) {
                msg.payload = new byte[payloadLen];
                buf.get(msg.payload);
            }

        } else if (type == MSG_ERROR && payload != null) {
            byte[] reasonBytes = new byte[payload.length - 1];
            System.arraycopy(payload, 1, reasonBytes, 0,
                             reasonBytes.length);
            msg.errorMsg = new String(reasonBytes, "UTF-8");
        }

        return msg;
    }
}