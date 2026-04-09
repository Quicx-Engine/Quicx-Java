package dev.quicx.internal;

import java.io.IOException;
import java.net.Socket;

public class Connection {

    private final String host;
    private final int    port;

    private Socket   socket;
    private Protocol protocol;

    public Connection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    // connect to daemon
    public void connect() throws IOException {
        socket   = new Socket(host, port);
        socket.setTcpNoDelay(true);   // no Nagle — send immediately
        socket.setKeepAlive(true);    // detect dead connections
        protocol = new Protocol(socket);
    }

    // disconnect
    public void close() {
        if (socket != null && !socket.isClosed()) {
            try { socket.close(); } catch (IOException ignored) {}
        }
        socket   = null;
        protocol = null;
    }

    // is the connection currently open
    public boolean isConnected() {
        return socket != null &&
               socket.isConnected() &&
               !socket.isClosed();
    }

    // get protocol for sending and receiving
    public Protocol protocol() {
        if (protocol == null)
            throw new IllegalStateException(
                "not connected — call connect() first");
        return protocol;
    }

    public String getHost() { return host; }
    public int    getPort() { return port; }
}