package com.ss.jms;

import com.ss.commons.DestinationConfiguration;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;

import javax.jms.*;

public class JMSProducer {

    private Connection connection;

    private Session session;

    private Logger logger;

    private DestinationConfiguration destination;

    private MessageProducer producer;

    private Destination dest;

    private int ackMode;

    private boolean isQueue;

    public JMSProducer(DestinationConfiguration destination, Logger logger, int ackMode, boolean isQueue) {
        this.logger = logger;
        this.destination = destination;
        this.ackMode = ackMode;
        this.isQueue = isQueue;
    }

    public Session getSession() {
        return session;
    }

    public void open() {
        try {
            String queue = destination.getProperty("queue");

            if (queue == null) {
                String msg = "The property queue must be specified";
                logger.error(msg);
                throw new RuntimeException(msg);
            }

            if (!isQueue) {
                dest = session.createTopic(queue);
            } else {
                dest = session.createQueue(queue);
            }

            ConnectionFactory cf = new ActiveMQConnectionFactory(destination.getUrl());
            this.connection = cf.createConnection();
            this.session = connection.createSession(false, ackMode);
            this.connection.start();

            producer = session.createProducer(dest);
        } catch (JMSException e) {
            String s = "Failed to create the JMS Connection";
            logger.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public void send(Message message) {
        try {
            producer.send(dest, message);
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
