package com.ss.kafka.consumer;

import com.google.common.base.Objects;

import java.io.Serializable;

public class Broker implements Serializable, Comparable<Broker> {
    public String host;
    public int port;

    // for kryo compatibility
    private Broker() {

    }

    public Broker(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Broker(String host) {
        this(host, 9092);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(host, port);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final Broker other = (Broker) obj;
        return Objects.equal(this.host, other.host) && Objects.equal(this.port, other.port);
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }

    public static Broker fromString(String host) {
        Broker hp;
        String[] spec = host.split(":");
        if (spec.length == 1) {
            hp = new Broker(spec[0]);
        } else if (spec.length == 2) {
            hp = new Broker(spec[0], Integer.parseInt(spec[1]));
        } else {
            throw new IllegalArgumentException("Invalid host specification: " + host);
        }
        return hp;
    }


    @Override
    public int compareTo(Broker o) {
        if (this.host.equals(o.host)) {
            return this.port - o.port;
        } else {
            return this.host.compareTo(o.host);
        }
    }
}
