package com.ss.commons;

import backtype.storm.tuple.Tuple;

public interface DestinationSelector {
    String select(Tuple message);
}
