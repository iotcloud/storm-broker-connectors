package com.ss.rabbitmq.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ss.rabbitmq.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RabbitMQBolt extends BaseRichBolt {
    private static Logger logger = LoggerFactory.getLogger(RabbitMQBolt.class);

    private Map<String, RabbitMQProducer> messageProducers = new HashMap<String, RabbitMQProducer>();

    private OutputCollector collector;

    private RabbitMQConfigurator configurator;

    private ErrorReporter reporter;

    public RabbitMQBolt(RabbitMQConfigurator configurator, ErrorReporter reporter) {
        this.configurator = configurator;
        this.reporter = reporter;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        for (RabbitMQDestination queue : configurator.getQueueName()) {
            RabbitMQProducer consumer = new RabbitMQProducer(configurator, reporter, queue);
            consumer.open();
            messageProducers.put(queue.getDestination(), consumer);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            RabbitMQMessage message = configurator.getMessageBuilder().serialize(tuple);
            String destination = configurator.getDestinationSelector().select(tuple);
            if (destination != null) {
                RabbitMQProducer producer = messageProducers.get(destination);

                if (producer != null) {
                    producer.send(message);
                }
            }
        } finally {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        this.configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void cleanup() {
        super.cleanup();

        for (RabbitMQProducer consumer : messageProducers.values()) {
            consumer.close();
        }
    }
}
