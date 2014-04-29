package com.ss.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class JMSConsumer {
    private ConnectionFactory cf;

    private Connection connection;

    private Session session;

    private int ackMode;

    private Logger logger;

    private BlockingQueue<JMSMessage> messages;

    private Destination destination;

    private Lock lock = new ReentrantLock();

    private String queue;

    public JMSConsumer(String queue, ConnectionFactory cf, int ackMode, Logger logger,
                       BlockingQueue<JMSMessage> messages, Destination destination) {
        this.cf = cf;
        this.ackMode = ackMode;
        this.logger = logger;
        this.messages = messages;
        this.destination = destination;
        this.queue = queue;
    }

    public void open() {
        try {
            this.connection = cf.createConnection();
            this.session = connection.createSession(false, this.ackMode);
            this.connection.start();

            MessageConsumer consumer = session.createConsumer(destination);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        messages.put(new JMSMessage(message, queue));
                    } catch (InterruptedException e) {
                        logger.warn("Error occurred while putting the message to queue", e);
                    }
                }
            });
        } catch (JMSException e) {
            String s = "Failed to create the JMS Connection";
            logger.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public void close() {
        try {
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException ignored) {
        }
    }

    private class RecoveryTask extends TimerTask {
        private final Logger LOG = LoggerFactory.getLogger(RecoveryTask.class);

        public void run() {
            JMSConsumer.this.lock.lock();
            try {
//                if (JMSConsumer.this.hasFailures()) {
//                    try {
//                        LOG.info("Recovering from a message failure.");
//                        JmsSpout.this.getSession().recover();
//                        JmsSpout.this.recovered();
//                    } catch (JMSException e) {
//                        LOG.warn("Could not recover jms session.", e);
//                    }
//                }
            } finally {
                JMSConsumer.this.lock.unlock();
            }
        }

    }

}
