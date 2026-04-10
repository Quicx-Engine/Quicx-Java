package dev.quicx;

public class QuicxException extends RuntimeException {
    public QuicxException(String message) {
        super(message);
    }
    public QuicxException(String message, Throwable cause) {
        super(message, cause);
    }
}