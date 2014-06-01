package com.ss.rabbitmq;

import backtype.storm.topology.OutputFieldsDeclarer;

import java.io.Serializable;
import java.util.List;

public interface RabbitMQConfigurator extends Serializable {
    String getURL();

    boolean isAutoAcking();

    int getPrefetchCount();

    boolean isReQueueOnFail();

    String getConsumerTag();

    List<RabbitMQDestination> getQueueName();

    MessageBuilder getMessageBuilder();

    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    int queueSize();

    RabbitMQDestinationSelector getDestinationSelector();
}
