package com.ss.kestrel;

import backtype.storm.topology.OutputFieldsDeclarer;

import java.io.Serializable;
import java.util.List;

public interface KestrelConfigurator extends Serializable {
    public static final int ACK_MESSAGE = 1;
    public static final int NO_ACK = 0;

    String getHost();

    int getPort();

    int ackMode();

    List<String> destinations();

    KestrelMessageBuilder getMessageBuilder();

    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    int queueSize();

    int expirationTime();

    long blackListTime();

    int timeOut();

    KestrelDestinationSelector getDestinationSelector();
}
