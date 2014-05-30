package com.ss.kestrel;

import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.List;

public interface KestrelConfigurator {
    public static final int ACK_MESSAGE = 1;
    public static final int NO_ACK = 0;

    int ackMode();

    List<KestrelDestination> destinations() throws Exception;

    KestrelMessageBuilder getMessageBuilder();

    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    int queueSize();
}
