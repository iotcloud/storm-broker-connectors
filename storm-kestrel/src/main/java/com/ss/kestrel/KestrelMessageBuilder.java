package com.ss.kestrel;

import java.util.List;

public interface KestrelMessageBuilder {
    List<Object> deSerialize(KestrelMessage message);
}
