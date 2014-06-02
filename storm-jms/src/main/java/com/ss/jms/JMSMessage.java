package com.ss.jms;

import javax.jms.Message;
import java.io.Serializable;

public class JMSMessage implements Serializable {
    private Message message;

    private String queue;

    public JMSMessage(Message message, String queue) {
        this.message = message;
        this.queue = queue;
    }

    public Message getMessage() {
        return message;
    }

    public String getQueue() {
        return queue;
    }
}
