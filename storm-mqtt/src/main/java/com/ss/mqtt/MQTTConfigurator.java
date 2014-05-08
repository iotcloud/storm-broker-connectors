package com.ss.mqtt;

import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.List;

public interface MQTTConfigurator {
    String getURL();

    List<String> getQueueName();

    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    MessageBuilder getMessageBuilder();

    int qosLevel();
}
