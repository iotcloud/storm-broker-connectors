package com.ss.commons;

import backtype.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.List;

public interface MessageBuilder extends Serializable {
    List<Object> deSerialize(Object message);

    Object serialize(Tuple tuple, Object context);
}
