package com.ss.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ZkBrokerReader implements IBrokerReader {

    public static final Logger LOG = LoggerFactory.getLogger(ZkBrokerReader.class);

    GlobalPartitionInformation cachedBrokers;
    DynamicBrokersReader reader;
    long lastRefreshTimeMs;
    long refreshMillis;

    public ZkBrokerReader(String topic, ZkHosts hosts) {
        try {
            reader = new DynamicBrokersReader(hosts.brokerZkStr, hosts.brokerZkPath, topic);
            cachedBrokers = reader.getBrokerInfo();
            lastRefreshTimeMs = System.currentTimeMillis();
            refreshMillis = hosts.refreshFreqSecs * 1000L;
        } catch (java.net.SocketTimeoutException e) {
            LOG.warn("Failed to update brokers", e);
        }

    }

    @Override
    public GlobalPartitionInformation getCurrentBrokers() {
        long currTime = System.currentTimeMillis();
        if (currTime > lastRefreshTimeMs + refreshMillis) {
            try {
                LOG.info("brokers need refreshing because " + refreshMillis + "ms have expired");
                cachedBrokers = reader.getBrokerInfo();
                lastRefreshTimeMs = currTime;
            } catch (java.net.SocketTimeoutException e) {
                LOG.warn("Failed to update brokers", e);
            }
        }
        return cachedBrokers;
    }

    @Override
    public void close() {
        reader.close();
    }
}
