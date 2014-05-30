package com.ss.kestrel;

public class KestrelMessage {
    private byte [] data; // required
    private long id; // required
    private KestrelDestination destination;
    private String queue;

    public KestrelMessage(byte [] data, long id, KestrelDestination destination, String queue) {
        this.data = data;
        this.id = id;
        this.destination = destination;
        this.queue = queue;
    }

    public KestrelDestination getDestination() {
        return destination;
    }

    public byte [] getData() {
        return data;
    }

    public long getId() {
        return id;
    }

    public String getQueue() {
        return queue;
    }
}
