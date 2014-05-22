package com.ss.mqtt.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ss.mqtt.MQTTConfigurator;
import com.ss.mqtt.MQTTMessage;
import com.ss.mqtt.MQTTProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MQTTBolt extends BaseRichBolt {
    private Logger logger;

    private MQTTConfigurator configurator;

    private transient OutputCollector collector;

    private Map<String, MQTTProducer> messageProducers = new HashMap<String, MQTTProducer>();

    public MQTTBolt(Logger logger, MQTTConfigurator configurator) {
        this.logger = logger;
        this.configurator = configurator;
    }

    public MQTTBolt(MQTTConfigurator configurator) {
        this(LoggerFactory.getLogger(MQTTBolt.class), configurator);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        for (String queue : configurator.getQueueName()) {
            MQTTProducer consumer = new MQTTProducer(logger, configurator.getURL(), queue);
            consumer.open();
            messageProducers.put(queue, consumer);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        MQTTMessage message = configurator.getMessageBuilder().serialize(tuple);

        MQTTProducer producer = messageProducers.get(message.getQueue());
        try {
            if (producer != null) {
                producer.send(message.getBody().toByteArray());
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

        for (MQTTProducer consumer : messageProducers.values()) {
            consumer.close();
        }
    }
}
