package com.ss.mqtt;

import org.fusesource.hawtbuf.Buffer;

public class Message {
    private Buffer body;

    private String queue;

    public Message(Buffer body, String queue) {
        this.body = body;
        this.queue = queue;
    }

    public Buffer getBody() {
        return body;
    }

    public String getQueue() {
        return queue;
    }
}
