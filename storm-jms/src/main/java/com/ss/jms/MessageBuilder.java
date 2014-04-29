package com.ss.jms;

import javax.jms.Message;
import java.util.List;

public interface MessageBuilder {
    List<Object> deSerialize(Message message);
}
