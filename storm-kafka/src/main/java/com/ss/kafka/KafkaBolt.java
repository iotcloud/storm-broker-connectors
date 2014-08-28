package com.ss.kafka;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ss.commons.*;
import com.ss.kafka.producer.KProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(KafkaBolt.class);

    private Map<String, KProducer> messageProducers = new HashMap<String, KProducer>();

    private OutputCollector collector;

    private BoltConfigurator configurator;

    private DestinationChanger destinationChanger;

    public KafkaBolt(BoltConfigurator configurator) {
        this.configurator = configurator;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        destinationChanger = configurator.getDestinationChanger();
        destinationChanger.registerListener(new DestinationChangeListener() {
            @Override
            public void addDestination(String name, DestinationConfiguration destination) {
                KProducer producer = new KProducer(destination, configurator.getProperties());
                producer.open();
                messageProducers.put(name, producer);
            }

            @Override
            public void removeDestination(String name) {
                KProducer producer = messageProducers.remove(name);
                if (producer != null) {
                    producer.close();
                }
            }
        });

        destinationChanger.start();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            KafkaMessage message = (KafkaMessage) configurator.getMessageBuilder().serialize(tuple, null);
            String destination = configurator.getDestinationSelector().select(tuple);
            if (destination != null) {
                KProducer producer = messageProducers.get(destination);
                if (producer != null) {
                    byte []key = message.getKey().getBytes();
                    producer.send(key, message.getBody());
                }
            }
        } finally {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        this.configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void cleanup() {
        destinationChanger.stop();

        super.cleanup();

        for (KProducer consumer : messageProducers.values()) {
            consumer.close();
        }
    }
}
