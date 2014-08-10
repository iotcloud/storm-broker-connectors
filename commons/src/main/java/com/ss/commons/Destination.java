package com.ss.commons;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Destination implements Serializable {
    private String url;

    private Map<String, String> properties = new HashMap<String, String>();

    public Destination(String url) {
        this.url = url;
    }

    public String getProperty(String name) {
        return properties.get(name);
    }

    public void addProperty(String name, String value) {
        this.properties.put(name, value);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getUrl() {
        return url;
    }
}
