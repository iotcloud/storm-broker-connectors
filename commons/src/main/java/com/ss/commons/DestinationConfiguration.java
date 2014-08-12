package com.ss.commons;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DestinationConfiguration implements Serializable {
    private String name;

    private String url;

    private Map<String, String> properties = new HashMap<String, String>();

    private String sensor;

    private String site;

    private boolean grouped;

    private String sensorId;

    public DestinationConfiguration(String name, String url, String site, String sensor) {
        this.name = name;
        this.url = url;
        this.site = site;
        this.sensor = sensor;
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

    public String getSensor() {
        return sensor;
    }

    public String getSite() {
        return site;
    }

    public boolean isGrouped() {
        return grouped;
    }

    public void setGrouped(boolean grouped) {
        this.grouped = grouped;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getSensorId() {
        return sensorId;
    }
}
