package com.ss.kafka.consumer;

import java.io.Serializable;
import java.util.List;

public class ConsumerConfig extends KafkaConfig implements Serializable {
    public List<String> zkServers = null;
    public String zkRoot = null;
    public String id = null;
    public long stateUpdateIntervalMs = 2000;

    public ConsumerConfig(BrokerHosts hosts, String topic, String zkRoot, String id) {
        super(hosts, topic);
        this.zkRoot = zkRoot;
        this.id = id;
    }
}
