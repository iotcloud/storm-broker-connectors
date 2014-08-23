package com.ss.kafka.producer;

import com.ss.commons.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class KProducer {
    private static Logger LOG = LoggerFactory.getLogger(KProducer.class);

    private Producer<byte[], byte []> producer;

    private DestinationConfiguration destination;

    private String queue;

    public KProducer(DestinationConfiguration destination) {
        this.destination = destination;
    }

    public void open() {
        Properties props = mapToProperties(destination.getProperties());
        ProducerConfig config = new ProducerConfig(props);
        producer = new Producer<byte[], byte []>(config);

        if (!destination.isGrouped()) {
            queue = destination.getSite() + "." + destination.getSensor() + "." + destination.getSensorId() + "." + destination.getProperty("queueName");
        } else {
            queue = destination.getSite() + "." + destination.getSensor() + "." + destination.getProperty("queueName");
        }
    }

    public void close() {
        producer.close();
    }

    public void send(byte []key, byte [] data) {
        producer.send(new KeyedMessage<byte[], byte[]>(queue, key, data));
    }

    public static Properties mapToProperties(Map<String, String> map) {
        Properties p = new Properties();
        Set<Map.Entry<String,String>> set = map.entrySet();
        for (Map.Entry<String,String> entry : set) {
            p.put(entry.getKey(), entry.getValue());
        }
        return p;
    }

}
