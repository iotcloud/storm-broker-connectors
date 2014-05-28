package com.ss.mqtt;

import backtype.storm.tuple.Tuple;

public interface DestinationSelector {
    String select(Tuple message);
}
