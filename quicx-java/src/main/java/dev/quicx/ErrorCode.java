package dev.quicx;

/** The reason code quicxd puts in an ERROR frame. */
public enum ErrorCode {

    /** The memory budget is used up (or, after READY, the worker pool is full). Try again later. */
    QUEUE_FULL(0x01),
    /** The frame could not be parsed. */
    INVALID_MESSAGE(0x02),
    /** The body was larger than 1,024 bytes. */
    PAYLOAD_TOO_BIG(0x03),
    /** The message type is not one the daemon knows. */
    UNKNOWN_TYPE(0x04),
    /** The client and the daemon speak different QXP versions. */
    VERSION_MISMATCH(0x05),
    /** A code this version of the client does not know. */
    UNKNOWN(-1);

    private final int code;

    ErrorCode(int code) {
        this.code = code;
    }

    /** The byte on the wire, or -1 for {@link #UNKNOWN}. */
    public int code() {
        return code;
    }

    /** The constant for a wire code; {@link #UNKNOWN} if there is none. */
    public static ErrorCode of(int code) {
        for (ErrorCode c : values())
            if (c.code == code) return c;
        return UNKNOWN;
    }
}
