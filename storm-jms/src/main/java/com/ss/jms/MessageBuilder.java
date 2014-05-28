package com.ss.jms;

import backtype.storm.tuple.Tuple;

import javax.jms.Message;
import java.util.List;

public interface MessageBuilder {
    List<Object> deSerialize(JMSMessage message);

    JMSMessage serialize(Tuple tuple, Object context);
}
