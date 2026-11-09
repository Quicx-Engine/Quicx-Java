package dev.quicx;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** A task as a worker receives it. */
public final class Task {

    private final int id;
    private final String type;
    private final byte[] payload;

    /**
     * Mostly for tests of your own handlers; the worker creates these.
     *
     * @param id      the id quicxd gave the task
     * @param type    its task type
     * @param payload its payload, may be empty
     */
    public Task(int id, String type, byte[] payload) {
        this.id = id;
        this.type = type;
        this.payload = payload == null ? new byte[0] : payload;
    }

    /** The id quicxd gave the task when it was submitted. */
    public int id() {
        return id;
    }

    /** The task type it was submitted with. */
    public String type() {
        return type;
    }

    /** A copy of the payload bytes. */
    public byte[] payload() {
        return payload.clone();
    }

    /** The payload decoded as UTF-8. */
    public String payloadAsString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    /** The payload length in bytes. */
    public int payloadLength() {
        return payload.length;
    }

    byte[] payloadNoCopy() {
        return payload;
    }

    @Override
    public String toString() {
        return "Task{id=" + id + ", type=" + type + ", " + payload.length + " bytes}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Task)) return false;
        Task t = (Task) o;
        return id == t.id && type.equals(t.type) && Arrays.equals(payload, t.payload);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * id + type.hashCode()) + Arrays.hashCode(payload);
    }
}
