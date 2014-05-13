package com.ss.kestrel;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class KestrelConsumer {
    private Logger logger;

    private KestrelThriftClient client = null;

    private BlockingQueue<KestrelMessage> messages;

    private String queue;

    public KestrelConsumer(Logger logger, String host, int port, String queue, BlockingQueue<KestrelMessage> messages) {
        if (logger == null) {
            this.logger = LoggerFactory.getLogger(KestrelConsumer.class);
        }
        this.messages = messages;
        this.queue = queue;

        try {
            client = new KestrelThriftClient(host, port);
        } catch (TException e) {
            String s = "Error occurred while creating the kestrel client";
            this.logger.error(s, e);
            throw new RuntimeException(s);
        }
    }

    public void open() {

    }

    public void ack(KestrelMessage message) {

    }

    public void fail(KestrelMessage message) {

    }

    public void close() {

    }
}
