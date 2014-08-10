package com.ss.jms.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ss.commons.BoltConfigurator;
import com.ss.commons.DestinationChangeListener;
import com.ss.commons.DestinationConfiguration;
import com.ss.jms.JMSProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import javax.jms.Session;
import java.util.HashMap;
import java.util.Map;

public class JMSBolt extends BaseRichBolt {
    private Logger logger;

    private BoltConfigurator configurator;

    private Map<String, JMSProducer> messageProducers = new HashMap<String, JMSProducer>();

    private OutputCollector collector;

    private int ackMode = Session.AUTO_ACKNOWLEDGE;

    private boolean isQueue = true;

    public JMSBolt(BoltConfigurator configurator, Logger logger) {
        if(configurator == null){
            throw new IllegalArgumentException("Configurator has not been set.");
        }

        this.configurator = configurator;
        if (logger == null) {
            this.logger = LoggerFactory.getLogger(JMSBolt.class);
        }

        String ackModeStringValue = configurator.getProperties().get("ackMode");

        if (ackModeStringValue != null) {
            ackMode = Integer.parseInt(ackModeStringValue);
        }

        String isQueueStringValue = configurator.getProperties().get("isQueue");
        if (isQueueStringValue != null) {
            isQueue = Boolean.parseBoolean(isQueueStringValue);
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        try {
            configurator.getDestinationChanger().registerListener(new DestinationChangeListener() {
                @Override
                public void addDestination(String name, DestinationConfiguration destination) {
                    JMSProducer consumer = new JMSProducer(destination, logger, ackMode, isQueue);
                    consumer.open();

                    messageProducers.put(name, consumer);
                }

                @Override
                public void removeDestination(String name) {
                    JMSProducer producer = messageProducers.remove(name);
                    if (producer != null) {
                        producer.close();
                    }
                }
            });
        } catch (Exception e) {
            logger.warn("Error creating JMS connection.", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String destination = configurator.getDestinationSelector().select(tuple);
            if (destination != null) {
                JMSProducer producer = messageProducers.get(destination);
                Message message = (Message) configurator.getMessageBuilder().serialize(tuple, producer.getSession());
                if (producer != null) {
                    producer.send(message);
                }
            } else {
                logger.warn("The DestinationSelector should give a valid destination");
            }
        } finally {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void cleanup() {
        super.cleanup();
        for (JMSProducer consumer : messageProducers.values()) {
            consumer.close();
        }
    }
}
