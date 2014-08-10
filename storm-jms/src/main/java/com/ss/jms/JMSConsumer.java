package com.ss.jms;

import com.ss.commons.DestinationConfiguration;
import com.ss.commons.MessageContext;
import org.apache.activemq.ActiveMQConnectionFactory;
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

    private Logger logger;

    private BlockingQueue<MessageContext> messages;

    private Lock lock = new ReentrantLock();

    private DestinationConfiguration destination;

    private int ackMode;

    private boolean isQueue = false;

    public JMSConsumer(DestinationConfiguration destination, Logger logger,
                       BlockingQueue<MessageContext> messages, int ackMode, boolean isQueue) {
        this.logger = logger;
        this.messages = messages;
        this.destination = destination;
        this.ackMode = ackMode;
        this.isQueue = isQueue;
    }

    public void open() {
        try {
            final String queue = destination.getProperty("queue");

            if (queue == null) {
                String msg = "The property queue must be specified";
                logger.error(msg);
                throw new RuntimeException(msg);
            }

            logger.info("Opening JMS Consumer for destination {}", queue);

            this.cf = new ActiveMQConnectionFactory(destination.getUrl());
            this.connection = cf.createConnection();
            this.session = connection.createSession(false, ackMode);
            this.connection.start();

            javax.jms.Destination dest;
            if (!isQueue) {
                dest = session.createTopic(queue);
            } else {
                dest = session.createQueue(queue);
            }

            MessageConsumer consumer = session.createConsumer(dest);
            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        messages.put(new MessageContext(message, destination.getName()));
                    } catch (InterruptedException e) {
                        logger.warn("Error occurred while putting the message to queue", e);
                    }
                }
            });
            logger.info("Finished Opening JMS Consumer for destination {}!", queue);
        } catch (JMSException e) {
            String s = "Failed to create the JMS Connection";
            logger.error(s, e);
            throw new RuntimeException(s, e);
        } catch (Throwable e) {
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
