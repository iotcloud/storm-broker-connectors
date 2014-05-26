package com.ss.mqtt;

public interface DestinationSelector {
    String select(MQTTMessage message);
}
