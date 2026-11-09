package dev.quicx;

/**
 * The connection to quicxd could not be opened, or broke before an answer
 * arrived. For a submit this means the outcome is unknown: the task may or
 * may not have been queued.
 */
public class QuicxConnectionException extends QuicxException {

    private static final long serialVersionUID = 1L;

    public QuicxConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
