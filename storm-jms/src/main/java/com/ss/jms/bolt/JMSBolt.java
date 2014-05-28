package com.ss.jms.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ss.jms.JMSConfigurator;
import com.ss.jms.JMSMessage;
import com.ss.jms.JMSProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import java.util.HashMap;
import java.util.Map;

public class JMSBolt extends BaseRichBolt {
    private Logger logger;

    private JMSConfigurator configurator;

    private Map<String, JMSProducer> messageProducers = new HashMap<String, JMSProducer>();

    private OutputCollector collector;

    public JMSBolt(JMSConfigurator configurator, Logger logger) {
        if(configurator == null){
            throw new IllegalArgumentException("Configurator has not been set.");
        }

        this.configurator = configurator;
        if (logger == null) {
            this.logger = LoggerFactory.getLogger(JMSBolt.class);
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        try {
            ConnectionFactory cf = this.configurator.connectionFactory();
            for (Map.Entry<String, Destination> e : configurator.destinations().entrySet()) {
                JMSProducer consumer = new JMSProducer(e.getKey(), cf, configurator.ackMode(), logger, e.getValue());
                consumer.open();

                messageProducers.put(e.getKey(), consumer);
            }
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
                JMSMessage message = (JMSMessage) configurator.getMessageBuilder().serialize(tuple, producer.getSession());
                if (producer != null) {
                    producer.send(message.getMessage());
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
