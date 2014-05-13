package com.ss.kestrel;

import java.nio.ByteBuffer;

public class KestrelMessage {
    private ByteBuffer data; // required
    private long id; // required
    private Destination destination;

    public KestrelMessage(ByteBuffer data, long id, Destination destination) {
        this.data = data;
        this.id = id;
        this.destination = destination;
    }

    public Destination getDestination() {
        return destination;
    }

    public ByteBuffer getData() {
        return data;
    }

    public long getId() {
        return id;
    }
}
