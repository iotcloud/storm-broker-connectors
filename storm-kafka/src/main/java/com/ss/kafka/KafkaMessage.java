package com.ss.kafka;

public class KafkaMessage {
    private String key;

    private byte [] body;

    public KafkaMessage(String key, byte[] body) {
        this.key = key;
        this.body = body;
    }

    public KafkaMessage(byte[] body) {
        this.body = body;
    }

    public String getKey() {
        return key;
    }

    public byte[] getBody() {
        return body;
    }
}
