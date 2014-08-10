package com.ss.jms;

import com.ss.commons.DestinationConfiguration;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;

import javax.jms.*;

public class JMSProducer {
    private ConnectionFactory cf;

    private Connection connection;

    private Session session;

    private Logger logger;

    private DestinationConfiguration destination;

    private String queue;

    private MessageProducer producer;

    private Destination dest;

    public JMSProducer(DestinationConfiguration destination, Logger logger) {
        this.logger = logger;
        this.destination = destination;
    }

    public Session getSession() {
        return session;
    }

    public void open() {
        try {
            boolean isQueue = false;
            queue = destination.getProperty("queue");

            if (queue == null) {
                String msg = "The property queue must be specified";
                logger.error(msg);
                throw new RuntimeException(msg);
            }

            String ackModeStringValue = destination.getProperty("ackMode");
            String isQueueStringValue = destination.getProperty("isQueue");
            int ackMode = Session.AUTO_ACKNOWLEDGE;
            if (ackModeStringValue != null) {
                ackMode = Integer.parseInt(ackModeStringValue);
            }
            if (isQueueStringValue != null) {
                isQueue = Boolean.parseBoolean(isQueueStringValue);
            }


            if (!isQueue) {
                dest = session.createTopic(queue);
            } else {
                dest = session.createQueue(queue);
            }

            this.cf = new ActiveMQConnectionFactory(destination.getUrl());
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
