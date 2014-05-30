package com.ss.kestrel;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class KestrelProducer {
    // by default we are going to black list a server for 30 secs
    private long blackListTime = 30000L;

    private static Logger logger = LoggerFactory.getLogger(KestrelProducer.class);

    private KestrelThriftClient client = null;

    private boolean run = true;

    private String destination;

    private long sleepTime = 0;

    private int expirationTime = 30000;

    private BlockingQueue<KestrelMessage> messages = new LinkedBlockingQueue<KestrelMessage>();

    private String host;

    private int port;

    public KestrelProducer(String host, int port, String destination) {
        this.destination = destination;
        this.host = host;
        this.port = port;
    }

    public void setBlackListTime(long blackListTime) {
        this.blackListTime = blackListTime;
    }

    public void setExpirationTime(int expirationTime) {
        this.expirationTime = expirationTime;
    }

    public void open() {
        Thread t = new Thread(new Worker());
        t.start();
    }

    public void close() {
        run = false;
        closeClient();
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

    public void send(KestrelMessage message) {
        try {
            messages.put(message);
        } catch (InterruptedException e) {
            logger.error("Error occurred while trying to put the message");
        }
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            while (run) {
                if (System.currentTimeMillis() < sleepTime) {
                    try {
                        getValidClient();
                    } catch (TException e) {
                        closeClient();
                        sleepTime = System.currentTimeMillis() + blackListTime;
                        break;
                    }

                    try {
                        List<ByteBuffer> sendBufferList = new ArrayList<ByteBuffer>();
                        // get the number of items in the outQUeue
                        int size = messages.size();
                        if (size > 0) {
                            for (int i = 0; i < size; i++) {
                                KestrelMessage input = messages.take();
                                if (input.getData() != null) {
                                    ByteBuffer byteBuffer = ByteBuffer.allocate((input.getData()).length);
                                    byteBuffer.put(input.getData());

                                    sendBufferList.add(byteBuffer);
                                } else {
                                    throw new RuntimeException("Expepected byte array after conversion");
                                }
                            }
                        } else {
                            KestrelMessage input = messages.take();
                            if (input != null) {
                                ByteBuffer byteBuffer = ByteBuffer.allocate((input.getData()).length);
                                byteBuffer.put(input.getData());

                                sendBufferList.add(byteBuffer);
                            } else {
                                throw new RuntimeException("Expected byte array after conversion");
                            }
                        }

                        try {
                            client.put(destination, sendBufferList, expirationTime);
                        } catch (TException e) {
                            closeClient();
                            sleepTime = System.currentTimeMillis() + blackListTime;
                        }
                    } catch (InterruptedException e) {
                        logger.error("Failed to add the message to the queue", e);
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
