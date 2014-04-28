package com.ss.rabbitmq;

import java.io.Serializable;

public interface ErrorReporter extends Serializable {
    void reportError(Throwable t);
}
