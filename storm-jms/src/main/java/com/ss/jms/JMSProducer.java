package com.ss.jms;

import org.slf4j.Logger;

import javax.jms.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class JMSProducer {
    private ConnectionFactory cf;

    private Connection connection;

    private Session session;

    private int ackMode;

    private Logger logger;

    private Destination destination;

    private Lock lock = new ReentrantLock();

    private String queue;

    private MessageProducer producer;

    public JMSProducer(String queue, ConnectionFactory cf, int ackMode, Logger logger,
                       Destination destination) {
        this.cf = cf;
        this.ackMode = ackMode;
        this.logger = logger;
        this.destination = destination;
        this.queue = queue;
    }

    public Session getSession() {
        return session;
    }

    public void open() {
        try {
            this.connection = cf.createConnection();
            this.session = connection.createSession(false, this.ackMode);
            this.connection.start();

            producer = session.createProducer(destination);
        } catch (JMSException e) {
            String s = "Failed to create the JMS Connection";
            logger.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public void send(Message message) {
        try {
            producer.send(destination, message);
        } catch (JMSException e) {
            String s = "Failed to send the JMS Message";
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

}
