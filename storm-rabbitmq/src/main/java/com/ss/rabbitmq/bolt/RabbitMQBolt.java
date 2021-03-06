package com.ss.rabbitmq.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ss.commons.BoltConfigurator;
import com.ss.commons.DestinationChangeListener;
import com.ss.commons.DestinationChanger;
import com.ss.commons.DestinationConfiguration;
import com.ss.rabbitmq.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RabbitMQBolt extends BaseRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(RabbitMQBolt.class);

    private Map<String, RabbitMQProducer> messageProducers = new HashMap<String, RabbitMQProducer>();

    private Map<String, String> pathToDestinations = new HashMap<String, String>();

    private OutputCollector collector;

    private BoltConfigurator configurator;

    private ErrorReporter reporter;

    private int prefetchCount = 0;

    private boolean isReQueueOnFail = false;

    private DestinationChanger destinationChanger;

    public RabbitMQBolt(BoltConfigurator configurator, ErrorReporter reporter) {
        this.configurator = configurator;
        this.reporter = reporter;

        String prefetchCountString = configurator.getProperties().get("prefectCount");
        if (prefetchCountString != null) {
            prefetchCount = Integer.parseInt(prefetchCountString);
        }

        String isReQueueOnFailString = configurator.getProperties().get("reQueue");
        if (isReQueueOnFailString != null) {
            isReQueueOnFail = Boolean.parseBoolean(isReQueueOnFailString);
        }
    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.collector = outputCollector;

        destinationChanger = configurator.getDestinationChanger();
        destinationChanger.registerListener(new DestinationChangeListener() {
            @Override
            public void addDestination(String name, DestinationConfiguration destination) {
                RabbitMQProducer producer = new RabbitMQProducer(reporter, destination, prefetchCount, isReQueueOnFail);
                producer.open();
                messageProducers.put(name, producer);
            }

            @Override
            public void removeDestination(String name) {
                RabbitMQProducer producer = messageProducers.remove(name);
                if (producer != null) {
                    producer.close();
                }
            }

            @Override
            public void addPathToDestination(String name, String path) {
                pathToDestinations.put(path, name);
            }

            @Override
            public void removePathToDestination(String name, String path) {
                pathToDestinations.remove(path);
            }
        });
        final int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        final int taskIndex = context.getThisTaskIndex();
        destinationChanger.setTask(taskIndex, totalTasks);

        destinationChanger.start();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            RabbitMQMessage message = (RabbitMQMessage) configurator.getMessageBuilder().serialize(tuple, null);
            String destination = configurator.getDestinationSelector().select(tuple);
            if (destination != null) {
                String dest = pathToDestinations.get(destination);
                if (dest != null) {
                    RabbitMQProducer producer = messageProducers.get(dest);
                    if (producer != null) {
                        producer.send(message);
                    } else {
                        LOG.warn("Cannot find producer for path {} and destination {}", dest, destination);
                    }
                } else {
                    LOG.warn("Cannot find path destination {}", destination);
                }
            } else {
                LOG.warn("Cannot find destination for tuple {}", tuple);
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
        destinationChanger.stop();

        super.cleanup();

        for (RabbitMQProducer consumer : messageProducers.values()) {
            consumer.close();
        }
    }
}
