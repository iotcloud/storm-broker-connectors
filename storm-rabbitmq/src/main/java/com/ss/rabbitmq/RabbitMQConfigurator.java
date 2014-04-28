package com.ss.rabbitmq;

import backtype.storm.topology.OutputFieldsDeclarer;
import com.rabbitmq.client.ConnectionFactory;

import java.io.Serializable;
import java.util.List;

public interface RabbitMQConfigurator extends Serializable {
    String getURL();

    boolean isAutoAcking();

    int getPrefetchCount();

    boolean isReQueueOnFail();

    String getConsumerTag();

    List<String> getQueueName();

    MessageBuilder getMessageBuilder();

    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    int queueSize();
}
