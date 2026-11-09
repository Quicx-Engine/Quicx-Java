package dev.quicx;

/** quicxd answered with an ERROR frame instead of accepting the request. */
public class QuicxRejectedException extends QuicxException {

    private static final long serialVersionUID = 1L;

    private final ErrorCode code;
    private final String reason;

    public QuicxRejectedException(ErrorCode code, String reason) {
        super("quicxd refused: " + reason + " (" + code + ")");
        this.code = code;
        this.reason = reason;
    }

    /** The error code from the frame. */
    public ErrorCode code() {
        return code;
    }

    /** The text quicxd sent with the code, for example "out of memory". */
    public String reason() {
        return reason;
    }

    /** True when the daemon's memory budget is full; the task can be sent again later. */
    public boolean isQueueFull() {
        return code == ErrorCode.QUEUE_FULL;
    }
}
