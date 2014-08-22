package com.ss.kafka.consumer;

public class ZkHosts implements BrokerHosts {
    private static final String DEFAULT_ZK_PATH = "/brokers";

    public String brokerZkStr = null;
    public String brokerZkPath = null; // e.g., /kafka/brokers
    public int refreshFreqSecs = 60;

    public ZkHosts(String brokerZkStr, String brokerZkPath) {
        this.brokerZkStr = brokerZkStr;
        this.brokerZkPath = brokerZkPath;
    }

    public ZkHosts(String brokerZkStr) {
        this(brokerZkStr, DEFAULT_ZK_PATH);
    }
}

