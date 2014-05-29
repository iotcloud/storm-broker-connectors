package com.ss.rabbitmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class RabbitMQSpout extends BaseRichSpout {
    private Logger logger;

    private ErrorReporter reporter;

    private RabbitMQConfigurator configurator;

    private transient SpoutOutputCollector collector;

    private Map<Long, String> queueMessageMap = new HashMap<Long, String>();

    private Map<String, MessageConsumer> messageConsumers = new HashMap<String, MessageConsumer>();

    private BlockingQueue<RabbitMQMessage> messages;

    public RabbitMQSpout(RabbitMQConfigurator configurator, ErrorReporter reporter) {
        this(configurator, reporter, LoggerFactory.getLogger(RabbitMQSpout.class));
    }

    public RabbitMQSpout(RabbitMQConfigurator configurator, ErrorReporter reporter, Logger logger) {
        this.configurator = configurator;
        this.reporter = reporter;
        this.logger = logger;
        this.messages = new ArrayBlockingQueue<RabbitMQMessage>(configurator.queueSize());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        for (RabbitMQDestination queue : configurator.getQueueName()) {
            MessageConsumer consumer = new MessageConsumer(messages, queue.getDestination(),
                    configurator, reporter, logger);
            consumer.openConnection();
            messageConsumers.put(queue.getDestination(), consumer);
        }
    }

    @Override
    public void nextTuple() {
        RabbitMQMessage message;
        while ((message = messages.poll()) != null) {
            List<Object> tuple = extractTuple(message);
            if (!tuple.isEmpty()) {
                collector.emit(tuple, message.getEnvelope().getDeliveryTag());
                if (!configurator.isAutoAcking()) {
                    queueMessageMap.put(message.getEnvelope().getDeliveryTag(), message.getQueue());
                }
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof Long) {
            if (!configurator.isAutoAcking()) {
                String name =  queueMessageMap.remove(msgId);
                if (name != null) {
                    MessageConsumer consumer = messageConsumers.get(name);
                    if (consumer != null) {
                        consumer.ackMessage((Long) msgId);
                    }
                }
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Long) {
            if (!configurator.isAutoAcking()) {
                String name =  queueMessageMap.remove(msgId);
                MessageConsumer consumer = messageConsumers.get(name);
                consumer.failMessage((Long) msgId);
            }
        }
    }

    @Override
    public void close() {
        for (MessageConsumer consumer : messageConsumers.values()) {
            consumer.closeConnection();
        }
        super.close();
    }

    public List<Object> extractTuple(RabbitMQMessage delivery) {
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
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
        MessageConsumer consumer = messageConsumers.get(delivery.getQueue());
        if (consumer != null) {
            consumer.deadLetter(deliveryTag);
        }

        return Collections.emptyList();
    }
}
