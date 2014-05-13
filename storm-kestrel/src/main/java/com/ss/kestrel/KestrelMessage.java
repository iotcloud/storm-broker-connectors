package com.ss.kestrel;

import java.nio.ByteBuffer;

public class KestrelMessage {
    private byte [] data; // required
    private long id; // required
    private Destination destination;

    public KestrelMessage(byte [] data, long id, Destination destination) {
        this.data = data;
        this.id = id;
        this.destination = destination;
    }

    public Destination getDestination() {
        return destination;
    }

    public byte [] getData() {
        return data;
    }

    public long getId() {
        return id;
    }
}
