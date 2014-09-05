package com.ss.commons;

import java.io.Serializable;

public interface DestinationChanger extends Serializable {
    public void start();

    public void stop();

    public void registerListener(DestinationChangeListener listener);

    public void setTask(int taskIndex, int totalTasks);

    public int getTaskIndex();

    public int getTotalTasks();
}
