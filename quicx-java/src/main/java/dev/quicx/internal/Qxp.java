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
    /** A SUBMIT with a value level in front of the body (quicxd 2.1.0 and later). */
    public static final byte SUBMIT_VALUED = 0x0D;

    /** Value levels: 0 is the least valuable, 15 is critical and never evicted. */
    public static final int MIN_LEVEL = 0;
    public static final int MAX_LEVEL = 15;
    /** The level quicxd gives a plain SUBMIT. */
    public static final int DEFAULT_LEVEL = 8;

    public static final int HEADER_SIZE = 6;

    /**
     * Largest body a default quicxd accepts (PROTOCOL_MAX_PAYLOAD in protocol.h).
     * A daemon built with {@code make MAX_PAYLOAD=N} accepts N; the client's
     * {@code maxBody} setting matches it.
     */
    public static final int MAX_BODY = 1024;

    /** Largest body any quicxd build accepts: a 64-byte record plus the body must fit a 16 KiB page. */
    public static final int MAX_BODY_LIMIT = 16_320;

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
        return submitBody(taskType, payload, MAX_BODY);
    }

    /** {@link #submitBody(String, byte[])} for a daemon that accepts bodies of up to {@code maxBody} bytes. */
    public static byte[] submitBody(String taskType, byte[] payload, int maxBody) {
        return body(-1, taskType, payload, maxBody);
    }

    /**
     * The body of a SUBMIT_VALUED: [level:1][type length:1][type][payload].
     * The level byte counts toward the daemon's body limit.
     *
     * @throws IllegalArgumentException if the level is outside 0-15, the type is invalid or the body is too large
     */
    public static byte[] valuedSubmitBody(int level, String taskType, byte[] payload, int maxBody) {
        checkLevel(level);
        return body(level, taskType, payload, maxBody);
    }

    /** @throws IllegalArgumentException unless {@code level} is between {@link #MIN_LEVEL} and {@link #MAX_LEVEL} */
    public static int checkLevel(int level) {
        if (level < MIN_LEVEL || level > MAX_LEVEL)
            throw new IllegalArgumentException("level must be " + MIN_LEVEL + "-" + MAX_LEVEL + ", not " + level);
        return level;
    }

    private static byte[] body(int level, String taskType, byte[] payload, int maxBody) {
        byte[] type = typeBytes(taskType);
        int payloadLen = payload == null ? 0 : payload.length;
        int head = level < 0 ? 1 : 2;   // [level] [type length]
        int len = head + type.length + payloadLen;
        if (len > maxBody) {
            throw new IllegalArgumentException("payload of " + payloadLen + " bytes is too large: with the task type '"
                    + taskType + "' at most " + (maxBody - head - type.length) + " bytes fit in one QXP frame of "
                    + maxBody + " bytes");
        }
        byte[] body = new byte[len];
        int off = 0;
        if (level >= 0) body[off++] = (byte) level;
        body[off++] = (byte) type.length;
        System.arraycopy(type, 0, body, off, type.length);
        if (payloadLen > 0) System.arraycopy(payload, 0, body, off + type.length, payloadLen);
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
