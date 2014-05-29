package com.ss.rabbitmq;

public class RabbitMQDestination {
    private String destination;

    private String exchange;

    private String routingKey;

    public RabbitMQDestination(String destination) {
        this.destination = destination;
    }

    public RabbitMQDestination(String destination, String exchange, String routingKey) {
        this.destination = destination;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    public String getDestination() {
        return destination;
    }

    public String getExchange() {
        return exchange;
    }

    public String getRoutingKey() {
        return routingKey;
    }
}
