package com.ss.commons;

import java.util.HashMap;
import java.util.Map;

public class MessageContext {
    private Object message;

    private String originDestination;

    private Map<String, Object> headers = new HashMap<String, Object>();

    private Map<String, Object> contextProps = new HashMap<String, Object>();

    private String id;

    public MessageContext(Object message, String originDestination) {
        this.message = message;
        this.originDestination = originDestination;
    }

    public MessageContext(String id, String originDestination, Object message) {
        this.id = id;
        this.originDestination = originDestination;
        this.message = message;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Object getMessage() {
        return message;
    }

    public String getOriginDestination() {
        return originDestination;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void addHeader(String name, Object value) {
        headers.put(name, value);
    }

    public Map<String, Object> getContextProps() {
        return contextProps;
    }

    public void contextProp(String name, Object value) {
        contextProps.put(name, value);
    }
}
