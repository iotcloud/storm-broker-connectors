package com.ss.mqtt;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.List;

public interface MessageBuilder extends Serializable {
    List<Object> deSerialize(MQTTMessage message);

    MQTTMessage serialize(Tuple tuple);
}
