package com.ss.commons;

import backtype.storm.topology.OutputFieldsDeclarer;

import java.io.Serializable;
import java.util.Map;

public interface BoltConfigurator extends Serializable {
    MessageBuilder getMessageBuilder();

    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    int queueSize();

    Map<String, String> getProperties();

    DestinationSelector getDestinationSelector();

    DestinationChanger getDestinationChanger();
}
