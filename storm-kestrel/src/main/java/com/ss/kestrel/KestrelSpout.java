package com.ss.kestrel;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class KestrelSpout extends BaseRichSpout {
    private Logger logger;

    private KestrelConfigurator configurator;

    private BlockingQueue<KestrelMessage> messages;

    private Map<Long, KestrelMessage> pendingMessages = new ConcurrentHashMap<Long, KestrelMessage>();

    private Map<KestrelDestination, KestrelConsumer> messageConsumers = new HashMap<KestrelDestination, KestrelConsumer>();

    private SpoutOutputCollector collector;

    public KestrelSpout(KestrelConfigurator configurator) {
        this.configurator = configurator;
    }

    public KestrelSpout(Logger logger, KestrelConfigurator configurator) {
        if (logger != null) {
            this.logger = logger;
        } else {
            this.logger = LoggerFactory.getLogger(KestrelSpout.class);
        }
        this.configurator = configurator;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.messages = new ArrayBlockingQueue<KestrelMessage>(configurator.queueSize());
        this.collector = spoutOutputCollector;

        try {
            for (KestrelDestination e : configurator.destinations()) {
                KestrelConsumer consumer = new KestrelConsumer(null, e, messages);
                consumer.open();

                messageConsumers.put(e, consumer);
            }
        } catch (Exception e) {
            logger.warn("Error creating JMS connection.", e);
        }
    }

    @Override
    public void nextTuple() {
        KestrelMessage message;
        while ((message = messages.poll()) != null) {
            List<Object> tuple = configurator.getMessageBuilder().deSerialize(message);
            if (!tuple.isEmpty()) {
                if (this.configurator.ackMode() != KestrelConfigurator.NO_ACK) {
                    logger.debug("Requesting acks.");
                    this.collector.emit(tuple, message.getId());

                    // at this point we successfully emitted. Store
                    // the message and message ID so we can do a
                    // JMS acknowledge later
                    this.pendingMessages.put(message.getId(), message);
                } else {
                    this.collector.emit(tuple, message.getId());
                }
            }
        }
    }

    @Override
    public void ack(Object msgId) {
        if (!(msgId instanceof Long)) {
            return;
        }

        KestrelMessage msg = this.pendingMessages.remove(msgId);
        if (msg != null) {
            KestrelConsumer consumer = messageConsumers.get(msg.getDestination());
            consumer.ack(msg);
            logger.debug("JMS Message acked: " + msgId);
        } else {
            if (configurator.ackMode() != KestrelConfigurator.NO_ACK) {
                logger.warn("Couldn't acknowledge unknown Kestrel message ID: " + msgId);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        if (!(msgId instanceof Long)) {
            return;
        }

        KestrelMessage msg = this.pendingMessages.remove(msgId);
        if (msg != null) {
            KestrelConsumer consumer = messageConsumers.get(msg.getDestination());
            consumer.fail(msg);
            logger.debug("JMS Message acked: " + msgId);
        } else {
            if (configurator.ackMode() != KestrelConfigurator.NO_ACK) {
                logger.warn("Couldn't acknowledge unknown Kestrel message ID: " + msgId);
            }
        }
    }

    @Override
    public void close() {
        for (KestrelConsumer consumer : messageConsumers.values()) {
            consumer.close();
        }

        super.close();
    }
}
