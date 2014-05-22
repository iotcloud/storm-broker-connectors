package com.ss.jms;

import backtype.storm.tuple.Tuple;

import javax.jms.Message;
import java.util.List;

public interface MessageBuilder {
    List<Object> deSerialize(Message message);

    String getQueue(Tuple tuple);

    JMSMessage serialize(Tuple tuple, Object context);
}
