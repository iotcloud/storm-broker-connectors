package com.ss.jms;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class JMSSpout extends BaseRichSpout {
    private Logger logger;

    private JMSConfigurator configurator;

    private BlockingQueue<JMSMessage> messages;

    private Map<String, Message> pendingMessages = new ConcurrentHashMap<String, Message>();

    private Map<String, JMSConsumer> messageConsumers = new HashMap<String, JMSConsumer>();

    private SpoutOutputCollector collector;

    public JMSSpout(JMSConfigurator configurator, Logger logger) {
        if(configurator == null){
            throw new IllegalArgumentException("Configurator has not been set.");
        }

        this.configurator = configurator;
        if (logger == null) {
            this.logger = LoggerFactory.getLogger(JMSSpout.class);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

//        Integer topologyTimeout = (Integer)conf.get("topology.message.timeout.secs");
//        // TODO fine a way to get the default timeout from storm, so we're not hard-coding to 30 seconds (it could change)
//        topologyTimeout = topologyTimeout == null ? 30 : topologyTimeout;
//        if( (topologyTimeout.intValue() * 1000 )> this.recoveryPeriod){
//            logger.warn("*** WARNING *** : " +
//                    "Recovery period ("+ this.recoveryPeriod + " ms.) is less then the configured " +
//                    "'topology.message.timeout.secs' of " + topologyTimeout +
//                    " secs. This could lead to a message replay flood!");
//        }

        this.messages = new ArrayBlockingQueue<JMSMessage>(configurator.queueSize());
        this.collector = spoutOutputCollector;

        try {
            ConnectionFactory cf = this.configurator.connectionFactory();
            for (Map.Entry<String, Destination> e : configurator.destinations().entrySet()) {
                JMSConsumer consumer = new JMSConsumer(cf, configurator.ackMode(), logger, messages);
                consumer.open();
            }
        } catch (Exception e) {
            logger.warn("Error creating JMS connection.", e);
        }

    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }

    @Override
    public void nextTuple() {
        JMSMessage message;
        while ((message = messages.poll()) != null) {
            List<Object> tuple = configurator.getMessageBuilder().deSerialize(message.getMessage());
            if (!tuple.isEmpty()) {
                try {
                    if (this.configurator.ackMode() != Session.AUTO_ACKNOWLEDGE
                            || (message.getMessage().getJMSDeliveryMode() != Session.AUTO_ACKNOWLEDGE)) {
                        logger.debug("Requesting acks.");
                        this.collector.emit(tuple, message.getMessage().getJMSMessageID());

                        // at this point we successfully emitted. Store
                        // the message and message ID so we can do a
                        // JMS acknowledge later
                        this.pendingMessages.put(message.getMessage().getJMSMessageID(), message.getMessage());
                    } else {
                        this.collector.emit(tuple);
                    }
                } catch (JMSException e) {
                    String s = "Failed to process the message";
                    logger.error(s, e);
                    throw new RuntimeException(s, e);
                }
            }
        }
    }
}
