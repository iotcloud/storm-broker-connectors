package com.ss.commons;

import java.io.Serializable;

public interface DestinationChangeListener extends Serializable {
    public void addDestination(String name, Destination destination);

    public void removeDestination(String name);
}
