package com.ss.kestrel;

import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.List;
import java.util.Map;

public interface KestrelConfigurator {
    public static final int ACK_MESSAGE = 1;
    public static final int NO_ACK = 0;

    int ackMode();

    Map<String, KestrelDestination> destinations();

    KestrelMessageBuilder getMessageBuilder();

    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    int queueSize();

    long expirationTime();

    long blackListTime();

    int timeOut();

    KestrelDestinationSelector getDestinationSelector();
}
