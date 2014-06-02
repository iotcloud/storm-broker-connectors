package com.ss.kestrel;

import java.io.Serializable;

public class KestrelMessage implements Serializable {
    private byte [] data; // required
    private long id; // required
    private String queue;

    public KestrelMessage(byte [] data, long id, String queue) {
        this.data = data;
        this.id = id;
        this.queue = queue;
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
