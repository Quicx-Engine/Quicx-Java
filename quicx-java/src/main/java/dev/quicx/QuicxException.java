package dev.quicx;

/**
 * Base class of everything this library throws. It is unchecked: catch it
 * where you can do something about it, for example
 * {@link QuicxRejectedException#isQueueFull()} to slow down.
 */
public class QuicxException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QuicxException(String message) {
        super(message);
    }

    public QuicxException(String message, Throwable cause) {
        super(message, cause);
    }
}
