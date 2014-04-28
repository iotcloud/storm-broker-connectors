package com.ss.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

public class Message {
    String consumerTag;
    Envelope envelope;
    AMQP.BasicProperties properties;
    byte[] body;

    String queue;

    public Message(String queue, String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        this.consumerTag = consumerTag;
        this.envelope = envelope;
        this.properties = properties;
        this.body = body;
        this.queue = queue;
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public Envelope getEnvelope() {
        return envelope;
    }

    public AMQP.BasicProperties getProperties() {
        return properties;
    }

    public byte[] getBody() {
        return body;
    }

    public String getQueue() {
        return queue;
    }
}
