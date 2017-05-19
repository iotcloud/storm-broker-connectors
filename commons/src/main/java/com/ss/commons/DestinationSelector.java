package com.ss.commons;

import com.twitter.heron.api.tuple.Tuple;

public interface DestinationSelector {
    String select(Tuple message);
}
