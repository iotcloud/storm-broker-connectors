package com.ss.kestrel;

import com.ss.kestrel.thrift.Item;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class KestrelConsumer {
    public static final int MAX_ITEMS = 1024;

    private Logger logger;

    private KestrelThriftClient client = null;

    private BlockingQueue<KestrelMessage> messages;

    private List<String> queues;

    private boolean run = true;

    private int timeoutMillis = 30000;

    private String host;
    private int port;

    public KestrelConsumer(Logger logger, String host, int port, List<String> queues, BlockingQueue<KestrelMessage> messages) {
        if (logger == null) {
            this.logger = LoggerFactory.getLogger(KestrelConsumer.class);
        }
        this.messages = messages;
        this.queues = queues;
        this.host = host;
        this.port = port;
    }

    public void setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public void open() {

    }

    public void ack(KestrelMessage message) {

    }

    public void fail(KestrelMessage message) {

    }

    public void close() {

    }

    private KestrelThriftClient getValidClient() throws TException {
        if (client == null) {
            client = new KestrelThriftClient(host, port);
        }
        return client;
    }

    private void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            while (run) {
                for (String q : queues) {
                    try {
                        List<Item> items = getValidClient().get(q, MAX_ITEMS, 0, timeoutMillis);
                        if (items != null) {
                            for (Item item :items) {
                                KestrelMessage m = new KestrelMessage(item.get_data(), item.get_id(), new Destination(host, port, q));
                                messages.put(m);
                            }
                        }
                    } catch (TException e) {
                        closeClient();
                    } catch (InterruptedException e) {

                    }
                }
            }
        }
    }
}
