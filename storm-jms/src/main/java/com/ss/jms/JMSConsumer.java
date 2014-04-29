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

    public JMSConsumer(ConnectionFactory cf, int ackMode, Logger logger, BlockingQueue<JMSMessage> messages) {
        this.cf = cf;
        this.ackMode = ackMode;
        this.logger = logger;
        this.messages = messages;
    }

    public void open() {
        try {
            this.connection = cf.createConnection();
            this.session = connection.createSession(false, this.ackMode);
            this.connection.start();


        } catch (JMSException e) {
            String s = "Failed to create the JMS Connection";
            logger.error(s, e);
            throw new RuntimeException(s, e);
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
