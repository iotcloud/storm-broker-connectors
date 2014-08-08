package com.ss.jms;

import backtype.storm.tuple.Tuple;

import java.util.List;

public interface MessageBuilder {
    List<Object> deSerialize(JMSMessage message);

    JMSMessage serialize(Tuple tuple, Object context);
}
