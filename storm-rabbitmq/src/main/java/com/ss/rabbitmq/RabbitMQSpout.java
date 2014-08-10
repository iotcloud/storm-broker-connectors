package com.ss.rabbitmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.ss.commons.DestinationChangeListener;
import com.ss.commons.DestinationConfiguration;
import com.ss.commons.MessageContext;
import com.ss.commons.SpoutConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class RabbitMQSpout extends BaseRichSpout {
    private Logger logger;

    private ErrorReporter reporter;

    private SpoutConfigurator configurator;

    private transient SpoutOutputCollector collector;

    private Map<String, String> queueMessageMap = new HashMap<String, String>();

    private Map<String, MessageConsumer> messageConsumers = new HashMap<String, MessageConsumer>();

    private BlockingQueue<MessageContext> messages;

    private int prefetchCount = 0;

    private boolean isReQueueOnFail = false;

    private boolean autoAck = true;

    public RabbitMQSpout(SpoutConfigurator configurator, ErrorReporter reporter) {
        this(configurator, reporter, LoggerFactory.getLogger(RabbitMQSpout.class));
    }

    public RabbitMQSpout(SpoutConfigurator configurator, ErrorReporter reporter, Logger logger) {
        this.configurator = configurator;
        this.reporter = reporter;
        this.logger = logger;
        this.messages = new ArrayBlockingQueue<MessageContext>(configurator.queueSize());

        String prefetchCountString = configurator.getProperties().get("prefectCount");
        if (prefetchCountString != null) {
            prefetchCount = Integer.parseInt(prefetchCountString);
        }

        String isReQueueOnFailString = configurator.getProperties().get("reQueue");
        if (isReQueueOnFailString != null) {
            isReQueueOnFail = Boolean.parseBoolean(isReQueueOnFailString);
        }

        String ackModeStringValue = configurator.getProperties().get("ackMode");
        if (ackModeStringValue != null && ackModeStringValue.equals("manual")) {
            autoAck = false;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        configurator.getDestinationChanger().registerListener(new DestinationChangeListener() {
            @Override
            public void addDestination(String name, DestinationConfiguration destination) {
                MessageConsumer consumer = new MessageConsumer(messages, destination, reporter, logger, prefetchCount, isReQueueOnFail, autoAck);
                consumer.openConnection();
                messageConsumers.put(name, consumer);
            }

            @Override
            public void removeDestination(String name) {
                MessageConsumer consumer = messageConsumers.remove(name);
                if (consumer != null) {
                    consumer.close();
                }
            }
        });
    }

    @Override
    public void nextTuple() {
        MessageContext message;
        while ((message = messages.poll()) != null) {
            List<Object> tuple = extractTuple(message);
            if (!tuple.isEmpty()) {
                collector.emit(tuple, message.getId());
                if (!autoAck) {
                    queueMessageMap.put(message.getId(), message.getOriginDestination());
                }
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof String) {
            if (!autoAck) {
                String name =  queueMessageMap.remove(msgId);
                if (name != null) {
                    MessageConsumer consumer = messageConsumers.get(name);
                    if (consumer != null) {
                        consumer.ackMessage(Long.parseLong(msgId.toString()));
                    }
                }
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof String) {
            if (!autoAck) {
                String name =  queueMessageMap.remove(msgId);
                MessageConsumer consumer = messageConsumers.get(name);
                consumer.failMessage(Long.parseLong(msgId.toString()));
            }
        }
    }

    @Override
    public void close() {
        for (MessageConsumer consumer : messageConsumers.values()) {
            consumer.close();
        }
        super.close();
    }

    public List<Object> extractTuple(MessageContext delivery) {
        String deliveryTag = delivery.getId();
        try {
            List<Object> tuple = configurator.getMessageBuilder().deSerialize(delivery);
            if (tuple != null && !tuple.isEmpty()) {
                return tuple;
            }
            String errorMsg = "Deserialization error for msgId " + deliveryTag;
            logger.warn(errorMsg);
            collector.reportError(new Exception(errorMsg));
        } catch (Exception e) {
            logger.warn("Deserialization error for msgId " + deliveryTag, e);
            collector.reportError(e);
        }
        MessageConsumer consumer = messageConsumers.get(delivery.getOriginDestination());
        if (consumer != null) {
            consumer.deadLetter(Long.parseLong(delivery.getId()));
        }
        return Collections.emptyList();
    }
}
