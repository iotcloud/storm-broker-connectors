package com.ss.mqtt;

import java.util.List;

public interface MessageBuilder {
    List<Object> deSerialize(Message message);
}
