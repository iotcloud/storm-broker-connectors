package com.ss.mqtt;

import org.fusesource.hawtbuf.Buffer;

public class Message {
    private Buffer body;

    private String queue;

    private String id;

    private Runnable onComplete;

    public Message(String id, Buffer body, String queue, Runnable onComplete) {
        this.body = body;
        this.queue = queue;
        this.onComplete = onComplete;
        this.id = id;
    }

    public Buffer getBody() {
        return body;
    }

    public String getQueue() {
        return queue;
    }

    public String getId() {
        return id;
    }

    public Runnable getOnComplete() {
        return onComplete;
    }
}
