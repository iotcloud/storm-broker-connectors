package com.ss.kestrel;

public class Destination {
    private String host;

    private int port;

    private String queue;

    public Destination(String host, int port, String queue) {
        this.host = host;
        this.port = port;
        this.queue = queue;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getQueue() {
        return queue;
    }
}
