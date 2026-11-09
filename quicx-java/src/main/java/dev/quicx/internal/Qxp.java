package dev.quicx.internal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * QXP, the Quicx Protocol: frame layout, limits and encoding.
 *
 * <p>Every frame is a 6-byte header (version, type, body length as a
 * big-endian uint32) followed by the body. This class is internal; its API
 * may change in any release.
 */
public final class Qxp {

    public static final byte VERSION = 1;

    public static final byte SUBMIT    = 0x01;
    public static final byte OK        = 0x02;
    public static final byte ERROR     = 0x03;
    public static final byte READY     = 0x04;
    public static final byte DONE      = 0x05;
    public static final byte FAILED    = 0x06;
    public static final byte TASK      = 0x07;
    public static final byte WAIT      = 0x08;
    public static final byte HEARTBEAT = 0x09;
    public static final byte PONG      = 0x0A;

    public static final int HEADER_SIZE = 6;

    /** Largest body quicxd accepts (PROTOCOL_MAX_PAYLOAD in protocol.h). */
    public static final int MAX_BODY = 1024;

    /** Longest task type in bytes (PROTOCOL_MAX_TYPE_LEN minus the terminating zero). */
    public static final int MAX_TYPE_BYTES = 31;

    /** Longest FAILED reason in bytes (PROTOCOL_MAX_FAIL_REASON_MSG minus the terminating zero). */
    public static final int MAX_REASON_BYTES = 63;

    /** quicxd never sends a body this large; anything bigger means the stream is broken. */
    static final int MAX_INCOMING_BODY = 64 * 1024;

    private Qxp() {}

    // ---- frames ------------------------------------------------------------

    /** Writes one frame and flushes. */
    public static void writeFrame(OutputStream out, byte type, byte[] body) throws IOException {
        int len = body == null ? 0 : body.length;
        byte[] frame = new byte[HEADER_SIZE + len];
        frame[0] = VERSION;
        frame[1] = type;
        putInt(frame, 2, len);
        if (len > 0) System.arraycopy(body, 0, frame, HEADER_SIZE, len);
        out.write(frame);
        out.flush();
    }

    /** Reads one whole frame; throws {@link EOFException} if the peer closed the connection. */
    public static Frame readFrame(InputStream in) throws IOException {
        byte[] header = new byte[HEADER_SIZE];
        readFully(in, header);
        if (header[0] != VERSION)
            throw new IOException("unsupported QXP version " + (header[0] & 0xFF));
        int len = getInt(header, 2);
        if (len < 0 || len > MAX_INCOMING_BODY)
            throw new IOException("frame body of " + Integer.toUnsignedString(len) + " bytes");
        byte[] body = new byte[len];
        readFully(in, body);
        return new Frame(header[1], body);
    }

    private static void readFully(InputStream in, byte[] buf) throws IOException {
        int off = 0;
        while (off < buf.length) {
            int n = in.read(buf, off, buf.length - off);
            if (n < 0) throw new EOFException("connection closed by quicxd");
            off += n;
        }
    }

    // ---- bodies ------------------------------------------------------------

    /**
     * The body of a SUBMIT: [type length:1][type][payload].
     *
     * @throws IllegalArgumentException if the type is invalid or the body would exceed {@link #MAX_BODY}
     */
    public static byte[] submitBody(String taskType, byte[] payload) {
        byte[] type = typeBytes(taskType);
        int payloadLen = payload == null ? 0 : payload.length;
        int len = 1 + type.length + payloadLen;
        if (len > MAX_BODY) {
            throw new IllegalArgumentException("payload of " + payloadLen + " bytes is too large: with the task type '"
                    + taskType + "' at most " + (MAX_BODY - 1 - type.length) + " bytes fit in one QXP frame");
        }
        byte[] body = new byte[len];
        body[0] = (byte) type.length;
        System.arraycopy(type, 0, body, 1, type.length);
        if (payloadLen > 0) System.arraycopy(payload, 0, body, 1 + type.length, payloadLen);
        return body;
    }

    /** The body of a DONE: [task id:4]. */
    public static byte[] doneBody(int taskId) {
        byte[] body = new byte[4];
        putInt(body, 0, taskId);
        return body;
    }

    /** The body of a FAILED: [task id:4][reason], the reason cut to {@link #MAX_REASON_BYTES}. */
    public static byte[] failedBody(int taskId, String reason) {
        byte[] text = truncateUtf8(reason == null ? "" : reason, MAX_REASON_BYTES);
        byte[] body = new byte[4 + text.length];
        putInt(body, 0, taskId);
        System.arraycopy(text, 0, body, 4, text.length);
        return body;
    }

    /**
     * Checks a task type and returns its UTF-8 bytes.
     *
     * @throws IllegalArgumentException if it is null, empty, longer than {@link #MAX_TYPE_BYTES} bytes or contains a zero byte
     */
    public static byte[] typeBytes(String taskType) {
        if (taskType == null || taskType.isEmpty())
            throw new IllegalArgumentException("task type must not be empty");
        byte[] type = taskType.getBytes(StandardCharsets.UTF_8);
        if (type.length > MAX_TYPE_BYTES)
            throw new IllegalArgumentException("task type '" + taskType + "' is " + type.length
                    + " bytes; QXP allows at most " + MAX_TYPE_BYTES);
        for (byte b : type)
            if (b == 0) throw new IllegalArgumentException("task type must not contain a zero character");
        return type;
    }

    // ---- helpers -----------------------------------------------------------

    /** UTF-8 bytes of {@code s}, cut at a character boundary so they fit in {@code maxBytes}. */
    public static byte[] truncateUtf8(String s, int maxBytes) {
        byte[] all = s.getBytes(StandardCharsets.UTF_8);
        if (all.length <= maxBytes) return all;
        int end = maxBytes;
        while (end > 0 && (all[end] & 0xC0) == 0x80) end--;   // never split a multi-byte character
        byte[] cut = new byte[end];
        System.arraycopy(all, 0, cut, 0, end);
        return cut;
    }

    public static String utf8(byte[] b, int off, int len) {
        return new String(b, off, len, StandardCharsets.UTF_8);
    }

    public static int getInt(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16) | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    static void putInt(byte[] b, int off, int v) {
        b[off]     = (byte) (v >>> 24);
        b[off + 1] = (byte) (v >>> 16);
        b[off + 2] = (byte) (v >>> 8);
        b[off + 3] = (byte) v;
    }
}
