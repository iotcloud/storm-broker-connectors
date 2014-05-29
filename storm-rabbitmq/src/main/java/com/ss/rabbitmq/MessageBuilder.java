package com.ss.rabbitmq;

import backtype.storm.tuple.Tuple;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.io.Serializable;
import java.util.List;

public interface MessageBuilder extends Serializable {
    List<Object> deSerialize(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body);

    RabbitMQMessage serialize(Tuple message);
}
