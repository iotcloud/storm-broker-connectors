package com.ss.kestrel;

import com.ss.kestrel.thrift.Item;
import com.ss.kestrel.thrift.Kestrel;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class KestrelProducer {
    // by default we are going to black list a server for 30 secs
    private long blackListTime = 30000L;

    public static final int MAX_ITEMS = 1024;

    private Logger logger;

    private KestrelThriftClient client = null;

    private boolean run = true;

    private int timeoutMillis = 30000;

    private KestrelDestination destination;

    private long sleepTime = 0;

    private BlockingQueue<KestrelMessage> messages;

    public KestrelProducer(Logger logger, KestrelDestination destination) {
        if (logger == null) {
            this.logger = LoggerFactory.getLogger(KestrelConsumer.class);
        }
        this.destination = destination;
    }

    public KestrelProducer(KestrelDestination destination) {
        this(null, destination);
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

    public void send(byte []message) {
        KestrelMessage kestrelMessage = new KestrelMessage(message, 0, null, null);

        try {
            messages.put(kestrelMessage);
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
                        Map<String, List<ByteBuffer>> sendMeStringListMap = new HashMap<String, List<ByteBuffer>>();
                        // List<ByteBuffer> sendMessages = new ArrayList<ByteBuffer>();
                        // get the number of items in the outQUeue
                        int size = messages.size();
                        if (size > 0) {
                            for (int i = 0; i < size; i++) {
                                KestrelMessage input = messages.take();
                                if (input.getData() != null) {
                                    ByteBuffer byteBuffer = ByteBuffer.allocate((input.getData()).length);
                                    byteBuffer.put(input.getData());

                                    addMessageForSend(sendMeStringListMap, input, byteBuffer);
                                } else {
                                    throw new RuntimeException("Expepected byte array after conversion");
                                }
                            }
                        } else {
                            KestrelMessage input = messages.take();
                            if (input != null) {
                                ByteBuffer byteBuffer = ByteBuffer.allocate(((byte[]) input.getData()).length);
                                byteBuffer.put(input.getData());

                                addMessageForSend(sendMeStringListMap, input, byteBuffer);
                            } else {
                                throw new RuntimeException("Expected byte array after conversion");
                            }
                        }

                        for (Map.Entry<String, List<ByteBuffer>> entry : sendMeStringListMap.entrySet()) {
                            try {
                                client.put(entry.getKey(), entry.getValue(), 0);
                            } catch (TException e) {
                                closeClient();
                                sleepTime = System.currentTimeMillis() + blackListTime;
                            }
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

    private static void addMessageForSend(Map<String, List<ByteBuffer>> sendMeStringListMap,
                                          KestrelMessage input, ByteBuffer byteBuffer) {
        List<ByteBuffer> queueMessageBuffer;
        if (!sendMeStringListMap.containsKey(input.getQueue())) {
            queueMessageBuffer = new ArrayList<ByteBuffer>();
        } else {
            queueMessageBuffer = sendMeStringListMap.get(input.getQueue());
        }
        queueMessageBuffer.add(byteBuffer);
    }
}
