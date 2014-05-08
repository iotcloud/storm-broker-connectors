package com.ss.mqtt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import org.fusesource.mqtt.client.QoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MQTTSpout extends BaseRichSpout {
    private Logger logger;

    private MQTTConfigurator configurator;

    private transient SpoutOutputCollector collector;

    private Map<String, Message> ackAwaitMessages = new HashMap<String, Message>();

    private Map<String, MessageConsumer> messageConsumers = new HashMap<String, MessageConsumer>();

    private BlockingQueue<Message> messages;

    public MQTTSpout(MQTTConfigurator configurator) {
        this(configurator, LoggerFactory.getLogger(MQTTSpout.class));
    }

    public MQTTSpout(MQTTConfigurator configurator, Logger logger) {
        this.configurator = configurator;
        this.logger = logger;
        this.messages = new ArrayBlockingQueue<Message>(configurator.queueSize());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        this.configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        for (String queue : configurator.getQueueName()) {
            MessageConsumer consumer = new MessageConsumer(logger, configurator.getURL(), messages, queue);
            consumer.open();
            messageConsumers.put(queue, consumer);
        }
    }

    @Override
    public void nextTuple() {
        Message message;
        while ((message = messages.poll()) != null) {
            List<Object> tuple = extractTuple(message);
            if (!tuple.isEmpty()) {
                collector.emit(tuple);
                if (configurator.qosLevel() != QoS.AT_MOST_ONCE) {
                    ackAwaitMessages.put(message.getId(), message);
                }
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof Long) {
            if (configurator.qosLevel() != QoS.AT_MOST_ONCE) {
                Message message =  ackAwaitMessages.remove(msgId.toString());
                MessageConsumer consumer = messageConsumers.get(message.getQueue());
                consumer.ack(message);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Long) {
            if (configurator.qosLevel() != QoS.AT_MOST_ONCE) {
                Message message =  ackAwaitMessages.remove(msgId.toString());
                MessageConsumer consumer = messageConsumers.get(message.getQueue());
                consumer.ack(message);
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

    public List<Object> extractTuple(Message delivery) {
        try {
            List<Object> tuple = configurator.getMessageBuilder().deSerialize(delivery);
            if (tuple != null && !tuple.isEmpty()) {
                return tuple;
            }
            String errorMsg = "Deserialization error for message";
            logger.warn(errorMsg);
            collector.reportError(new Exception(errorMsg));
        } catch (Exception e) {
            logger.warn("Deserialization error for msg", e);
            collector.reportError(e);
        }

        return Collections.emptyList();
    }
}
