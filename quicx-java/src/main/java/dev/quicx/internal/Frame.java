package dev.quicx.internal;

/** One QXP frame as read from the wire: its type byte and its body. */
public final class Frame {

    /** The message type, one of the {@link Qxp} type constants. */
    public final byte type;

    /** The body that followed the 6-byte header; never null, may be empty. */
    public final byte[] body;

    public Frame(byte type, byte[] body) {
        this.type = type;
        this.body = body;
    }
}
