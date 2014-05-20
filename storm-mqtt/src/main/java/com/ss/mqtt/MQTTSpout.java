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

    private Map<String, MQTTMessage> ackAwaitMessages = new HashMap<String, MQTTMessage>();

    private Map<String, MQTTConsumer> messageConsumers = new HashMap<String, MQTTConsumer>();

    private BlockingQueue<MQTTMessage> messages;

    public MQTTSpout(MQTTConfigurator configurator) {
        this(configurator, LoggerFactory.getLogger(MQTTSpout.class));
    }

    public MQTTSpout(MQTTConfigurator configurator, Logger logger) {
        this.configurator = configurator;
        if (logger != null) {
            this.logger = logger;
        } else {
            this.logger = LoggerFactory.getLogger(MQTTSpout.class);
        }
        this.messages = new ArrayBlockingQueue<MQTTMessage>(configurator.queueSize());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        this.configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        for (String queue : configurator.getQueueName()) {
            MQTTConsumer consumer = new MQTTConsumer(logger, configurator.getURL(), messages, queue);
            consumer.open();
            messageConsumers.put(queue, consumer);
        }
    }

    @Override
    public void nextTuple() {
        MQTTMessage message;
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
                MQTTMessage message =  ackAwaitMessages.remove(msgId.toString());
                MQTTConsumer consumer = messageConsumers.get(message.getQueue());
                consumer.ack(message);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Long) {
            if (configurator.qosLevel() != QoS.AT_MOST_ONCE) {
                MQTTMessage message =  ackAwaitMessages.remove(msgId.toString());
                MQTTConsumer consumer = messageConsumers.get(message.getQueue());
                consumer.ack(message);
            }
        }
    }

    @Override
    public void close() {
        for (MQTTConsumer consumer : messageConsumers.values()) {
            consumer.close();
        }
        super.close();
    }

    public List<Object> extractTuple(MQTTMessage delivery) {
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
