package com.ss.jms;

import backtype.storm.tuple.Tuple;

public interface JMSDestinationSelector {
    String select(Tuple message);
}
