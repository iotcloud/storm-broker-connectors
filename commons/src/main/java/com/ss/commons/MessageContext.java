package com.ss.commons;

public class MessageContext {
    private Object message;

    private String originDestination;

    public MessageContext(Object message, String originDestination) {
        this.message = message;
        this.originDestination = originDestination;
    }

    public Object getMessage() {
        return message;
    }

    public String getOriginDestination() {
        return originDestination;
    }
}
