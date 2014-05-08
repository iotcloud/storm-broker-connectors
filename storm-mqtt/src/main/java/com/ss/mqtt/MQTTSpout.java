package com.ss.mqtt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class MQTTSpout extends BaseRichSpout {
    private Logger logger;

    private MQTTConfigurator configurator;

    private transient SpoutOutputCollector collector;

    private Map<Long, String> queueMessageMap = new HashMap<Long, String>();

    private Map<String, MessageConsumer> messageConsumers = new HashMap<String, MessageConsumer>();

    private BlockingQueue<Message> messages;

    public MQTTSpout(MQTTConfigurator configurator) {
        this(configurator, LoggerFactory.getLogger(MQTTSpout.class));
    }

    public MQTTSpout(MQTTConfigurator configurator, Logger logger) {
        this.configurator = configurator;
        this.logger = logger;
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
                queueMessageMap.put(12l, message.getQueue());
            }
        }
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
//        MessageConsumer consumer = messageConsumers.get(delivery.getQueue());
//        if (consumer != null) {
//            consumer.deadLetter(deliveryTag);
//        }

        return Collections.emptyList();
    }
}
