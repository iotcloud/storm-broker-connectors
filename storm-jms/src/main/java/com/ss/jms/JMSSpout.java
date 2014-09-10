package com.ss.jms;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.ss.commons.*;
import com.ss.commons.DestinationConfiguration;
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

    private SpoutConfigurator configurator;

    private BlockingQueue<MessageContext> messages;

    private Map<String, MessageContext> pendingMessages = new ConcurrentHashMap<String, MessageContext>();

    private Map<String, JMSConsumer> messageConsumers = new HashMap<String, JMSConsumer>();

    private SpoutOutputCollector collector;

    private int ackMode = Session.AUTO_ACKNOWLEDGE;

    private boolean isQueue = true;

    public JMSSpout(SpoutConfigurator configurator, Logger logger) {
        if(configurator == null){
            throw new IllegalArgumentException("Configurator has not been set.");
        }

        this.configurator = configurator;
        if (logger == null) {
            this.logger = LoggerFactory.getLogger(JMSSpout.class);
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
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.messages = new ArrayBlockingQueue<MessageContext>(configurator.queueSize());
        this.collector = spoutOutputCollector;

        configurator.getDestinationChanger().registerListener(new DestinationChangeListener() {
            @Override
            public void addDestination(String name, DestinationConfiguration destination) {
                JMSConsumer consumer = new JMSConsumer(destination, logger, messages, ackMode, isQueue);
                messageConsumers.put(name, consumer);
            }

            @Override
            public void removeDestination(String name) {
                JMSConsumer consumer = messageConsumers.remove(name);
                if (consumer != null) {
                    consumer.close();
                }
            }

            @Override
            public void addPathToDestination(String name, String path) {

            }

            @Override
            public void removePathToDestination(String name, String path) {

            }
        });
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
        MessageContext msg = this.pendingMessages.remove(msgId.toString());
        if (msg != null) {
            try {
                if (msg.getMessage() instanceof Message) {
                    ((Message) msg.getMessage()).acknowledge();
                }
                logger.debug("JMS Message acked: " + msgId);
            } catch (JMSException e) {
                logger.warn("Error acknowldging JMS message: " + msgId, e);
            }
        } else {
            if (getAckMode() != Session.AUTO_ACKNOWLEDGE) {
                logger.warn("Couldn't acknowledge unknown JMS message ID: " + msgId);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        MessageContext msg = this.pendingMessages.remove(msgId.toString());
        if (msg != null) {
            try {
                if (msg.getMessage() instanceof Message) {
                    ((Message) msg.getMessage()).acknowledge();
                }
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
        MessageContext msg;
        while ((msg = messages.poll()) != null) {
            List<Object> tuple = configurator.getMessageBuilder().deSerialize(msg);
            if (!tuple.isEmpty()) {
                Message message;
                if (msg.getMessage() instanceof Message) {
                    message = (Message) msg.getMessage();
                } else {
                    continue;
                }

                try {
                    if (getAckMode() != Session.AUTO_ACKNOWLEDGE
                            || (message.getJMSDeliveryMode() != Session.AUTO_ACKNOWLEDGE)) {
                        logger.debug("Requesting acks.");
                        this.collector.emit(tuple, message.getJMSMessageID());

                        // at this point we successfully emitted. Store
                        // the message and message ID so we can do a
                        // JMS acknowledge later
                        this.pendingMessages.put(message.getJMSMessageID(), msg);
                    } else {
                        this.collector.emit(tuple, message.getJMSMessageID());
                    }
                } catch (JMSException e) {
                    String s = "Failed to process the message";
                    logger.error(s, e);
                    throw new RuntimeException(s, e);
                }
            }
        }
    }

    private int getAckMode() {
        return ackMode;
    }
}
