package com.ss.kestrel;

import java.util.ArrayList;
import java.util.List;

public class Destination {
    private String host;

    private int port;

    private List<String> queue = new ArrayList<String>();

    public Destination(String host, int port, List<String> queue) {
        this.host = host;
        this.port = port;
        this.queue.addAll(queue);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public List<String> getQueues() {
        return queue;
    }
}
