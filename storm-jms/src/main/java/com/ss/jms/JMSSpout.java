package com.ss.jms;

import backtype.storm.messaging.local;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;
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
        this.messages = new ArrayBlockingQueue<JMSMessage>(configurator.queueSize());
        this.collector = spoutOutputCollector;

        try {
            ConnectionFactory cf = this.configurator.connectionFactory();
            for (Map.Entry<String, Destination> e : configurator.destinations().entrySet()) {
                JMSConsumer consumer = new JMSConsumer(e.getKey(), cf, configurator.ackMode(), logger, messages, e.getValue());
                consumer.open();

                messageConsumers.put(e.getKey(), consumer);
            }
        } catch (Exception e) {
            logger.warn("Error creating JMS connection.", e);
        }
    }

    @Override
    public void close() {
        super.close();
        for (JMSConsumer consumer : messageConsumers.values()) {
            consumer.close();
        }
    }

    @Override
    public void ack(Object msgId) {
        Message msg = this.pendingMessages.remove(msgId.toString());
        if (msg != null) {
            try {
                msg.acknowledge();
                logger.debug("JMS Message acked: " + msgId);
            } catch (JMSException e) {
                logger.warn("Error acknowldging JMS message: " + msgId, e);
            }
        } else {
            if (configurator.ackMode() != Session.AUTO_ACKNOWLEDGE) {
                logger.warn("Couldn't acknowledge unknown JMS message ID: " + msgId);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        Message msg = this.pendingMessages.remove(msgId.toString());
        if (msg != null) {
            try {
                msg.acknowledge();
                logger.debug("JMS Message acked: " + msgId);
            } catch (JMSException e) {
                logger.warn("Error acknowldging JMS message: " + msgId, e);
            }
        } else {
            logger.warn("Couldn't acknowledge unknown JMS message ID: " + msgId);
        }
    }

    @Override
    public void nextTuple() {
        JMSMessage message;
        while ((message = messages.poll()) != null) {
//        message = messages.poll();
//        if (message != null) {
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
                        this.collector.emit(tuple, message.getMessage().getJMSMessageID());
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
