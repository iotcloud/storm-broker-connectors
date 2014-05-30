package com.ss.kestrel;

import java.util.ArrayList;
import java.util.List;

public class KestrelDestination {
    private String host;

    private int port;

    private List<String> queue = new ArrayList<String>();

    public KestrelDestination(String host, int port, List<String> queue) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KestrelDestination that = (KestrelDestination) o;

        if (port != that.port) return false;
        if (host != null ? !host.equals(that.host) : that.host != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host != null ? host.hashCode() : 0;
        result = 31 * result + port;
        return result;
    }
}
