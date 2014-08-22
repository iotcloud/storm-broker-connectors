package com.ss.kafka.consumer;

public interface IBrokerReader {
    GlobalPartitionInformation getCurrentBrokers();
    void close();
}
