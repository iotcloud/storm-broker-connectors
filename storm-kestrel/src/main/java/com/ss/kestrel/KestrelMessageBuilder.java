package com.ss.kestrel;

import backtype.storm.tuple.Tuple;

import java.util.List;

public interface KestrelMessageBuilder {
    List<Object> deSerialize(KestrelMessage message);

    KestrelMessage serialize(Tuple tuple);
}
