package com.ss.rabbitmq;

import backtype.storm.tuple.Tuple;

public interface RabbitMQDestinationSelector {
    String select(Tuple message);
}
