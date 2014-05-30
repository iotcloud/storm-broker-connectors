package com.ss.kestrel.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ss.kestrel.KestrelConfigurator;
import com.ss.kestrel.KestrelDestination;
import com.ss.kestrel.KestrelMessage;
import com.ss.kestrel.KestrelProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KestrelBolt extends BaseRichBolt {
    private static Logger logger = LoggerFactory.getLogger(KestrelBolt.class);

    private KestrelConfigurator configurator;

    private transient OutputCollector collector;

    private Map<KestrelDestination, KestrelProducer> messageProducers = new HashMap<KestrelDestination, KestrelProducer>();

    public KestrelBolt(KestrelConfigurator configurator) {
        this.configurator = configurator;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        for (KestrelDestination queue : configurator.destinations()) {
            KestrelProducer consumer = new KestrelProducer(queue);
            consumer.setBlackListTime(configurator.blackListTime());
            consumer.setTimeoutMillis(configurator.timeOut());

            consumer.open();
            messageProducers.put(queue, consumer);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        KestrelMessage message = configurator.getMessageBuilder().serialize(tuple);

        String destination = configurator.getDestinationSelector().select(tuple);
        try {
            if (destination != null) {
                KestrelProducer producer = messageProducers.get(destination);

                if (producer != null) {
                    producer.send(message);
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
        super.cleanup();

        for (KestrelProducer consumer : messageProducers.values()) {
            consumer.close();
        }
    }
}
