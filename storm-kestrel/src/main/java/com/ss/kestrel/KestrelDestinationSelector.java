package com.ss.kestrel;

import backtype.storm.tuple.Tuple;

public interface KestrelDestinationSelector {
    String select(Tuple message);
}
