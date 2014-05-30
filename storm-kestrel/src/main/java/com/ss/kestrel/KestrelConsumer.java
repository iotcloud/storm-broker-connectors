package com.ss.kestrel;

import com.ss.kestrel.thrift.Item;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class KestrelConsumer {
    // by default we are going to black list a server for 30 secs
    private long blackListTime = 30000L;

    public static final int MAX_ITEMS = 1024;

    private Logger logger;

    private KestrelThriftClient client = null;

    private BlockingQueue<KestrelMessage> messages;

    private boolean run = true;

    private int timeoutMillis = 30000;

    private KestrelDestination destination;

    private long sleepTime = 0;

    public KestrelConsumer(Logger logger, KestrelDestination destination, BlockingQueue<KestrelMessage> messages) {
        if (logger == null) {
            this.logger = LoggerFactory.getLogger(KestrelConsumer.class);
        }
        this.messages = messages;
        this.destination = destination;
    }

    public KestrelConsumer(KestrelDestination destination, BlockingQueue<KestrelMessage> messages) {
        this(null, destination, messages);
    }

    public void setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public void setBlackListTime(long blackListTime) {
        this.blackListTime = blackListTime;
    }

    public void open() {
        Thread t = new Thread(new Worker());
        t.start();
    }

    public void ack(KestrelMessage message) {
        try {
            KestrelThriftClient thriftClient = getValidClient();
            Set<Long> set = new HashSet<Long>();
            set.add(message.getId());
            thriftClient.confirm(message.getQueue(), set);
        } catch (TException e) {
            logger.error("Failed to ack the message with id {}", message.getId(), e);
            closeClient();
        }
    }

    public void fail(KestrelMessage message) {
        try {
            KestrelThriftClient thriftClient = getValidClient();
            Set<Long> set = new HashSet<Long>();
            set.add(message.getId());
            thriftClient.abort(message.getQueue(), set);
        } catch (TException e) {
            logger.error("Failed to abort the message with id {}", message.getId(), e);
            closeClient();
        }
    }

    public void close() {
        closeClient();
    }

    private KestrelThriftClient getValidClient() throws TException {
        if (client == null) {
            client = new KestrelThriftClient(destination.getHost(), destination.getPort());
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
                if (System.currentTimeMillis() < sleepTime) {
                    boolean queueWorking = false;
                    for (String q : destination.getQueues()) {
                        try {
                            getValidClient();
                        } catch (TException e) {
                            closeClient();
                            sleepTime = System.currentTimeMillis() + blackListTime;
                            break;
                        }


                        List<Item> items;
                        try {
                            items = client.get(q, MAX_ITEMS, 0, timeoutMillis);
                            queueWorking = true;
                            if (items != null) {
                                for (Item item : items) {
                                    KestrelMessage m = new KestrelMessage(item.get_data(), item.get_id(), destination, q);
                                    messages.put(m);
                                }
                            }
                        } catch (TException e) {
                            logger.debug("Error retrieving messages from queue {} and host {} port {}", q, destination.getHost(), destination.getPort());
                            closeClient();
                        } catch (InterruptedException e) {
                            logger.error("Failed to add the message to the queue", e);
                        }
                    }
                    // if a single queue isn't working we are going to sleep
                    if (!queueWorking) {
                        closeClient();
                        sleepTime = System.currentTimeMillis() + blackListTime;
                    }
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }
}
