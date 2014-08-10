package com.ss.commons;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DestinationConfiguration implements Serializable {
    private String name;

    private String url;

    private Map<String, String> properties = new HashMap<String, String>();

    public DestinationConfiguration(String name, String url) {
        this.url = url;
        this.name = name;
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

    public String getName() {
        return name;
    }
}
