package com.ss.rabbitmq;

import com.rabbitmq.client.*;
import com.ss.commons.DestinationConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RabbitMQProducer {
    private Connection connection;

    private Channel channel;

    private QueueingConsumer consumer;

    private String consumerTag;

    private ErrorReporter reporter;

    private Logger logger = LoggerFactory.getLogger(RabbitMQProducer.class);

    private DestinationConfiguration destination;

    private String exchangeName;

    private String queue;

    private String routingKey;

    private int prefetchCount;

    private boolean isReQueueOnFail = false;

    public RabbitMQProducer(ErrorReporter reporter,
                           DestinationConfiguration destination, int prefetchCount, boolean isReQueueOnFail) {
        this.reporter = reporter;
        this.destination = destination;
        this.prefetchCount = prefetchCount;
        this.isReQueueOnFail = isReQueueOnFail;

        readProps();
    }

    private void reset() {
        consumerTag = null;
    }

    private void reInitIfNecessary() {
        if (consumerTag == null || consumer == null) {
            close();
            open();
        }
    }

    private void readProps() {
        if (destination.getProperty("queueName") == null) {
            String msg = "The property queue must be specified";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        if (!destination.isGrouped()) {
            queue = destination.getSite() + "." + destination.getSensor() + "." + destination.getSensorId() + "." + destination.getProperty("queueName");
        } else {
            queue = destination.getSite() + "." + destination.getSensor() + "." + destination.getProperty("queueName");
        }

        if (!destination.isGrouped()) {
            routingKey = destination.getSite() + "." + destination.getSensor() + "." + destination.getSensorId() + "." + destination.getProperty("routingKey");
        } else {
            routingKey = destination.getSite() + "." + destination.getSensor() + "." + destination.getProperty("routingKey");
        }
        exchangeName = destination.getProperty("exchange");
    }

    public void close() {
        logger.info("Closing channel to queue {}", queue);
        try {
            if (channel != null && channel.isOpen()) {
                channel.queueDelete(queue, true, false);
                if (consumerTag != null) {
                    channel.basicCancel(consumerTag);
                }
                channel.close();
            }
        } catch (Exception e) {
            logger.debug("error closing channel and/or cancelling consumer", e);
        }
        try {
            logger.info("closing connection to rabbitmq: " + connection);
            connection.close();
        } catch (Exception e) {
            logger.debug("error closing connection", e);
        }
        consumer = null;
        consumerTag = null;
        channel = null;
        connection = null;
    }

    public void open() {
        try {
            connection = createConnection();
            channel = connection.createChannel();

            if (prefetchCount > 0) {
                logger.info("setting basic.qos / prefetch count to " + prefetchCount + " for " + queue);
                channel.basicQos(prefetchCount);
            }

            channel.exchangeDeclare(exchangeName, "direct", false);
            channel.queueDeclare(queue, false, false, true, null);
            channel.queueBind(queue, exchangeName, routingKey);
        } catch (Exception e) {
            reset();
            logger.error("could not open listener on queue " + queue);
            reporter.reportError(e);
        }
    }

    public void send(RabbitMQMessage message) {
        try {
            channel.basicPublish(exchangeName, routingKey,
                    message.getProperties(),
                    message.getBody());
        } catch (IOException e) {
            logger.error("Failed to ");
        }
    }

    private Connection createConnection() throws IOException {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setUri(destination.getUrl());
            Connection connection = connectionFactory.newConnection();
            connection.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException cause) {
                    logger.error("shutdown signal received", cause);
                    reporter.reportError(cause);
                    reset();
                }
            });
            logger.info("connected to rabbitmq: " + connection + " for " + queue);
            return connection;

        } catch (Exception e) {
            logger.info("connected to rabbitmq: " + connection + " for " + queue);
            reporter.reportError(e);
            return null;
        }
    }

    public void ackMessage(Long msgId) {
        try {
            channel.basicAck(msgId, false);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to ack message", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("could not ack for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void failMessage(Long msgId) {
        if (isReQueueOnFail) {
            failWithRedelivery(msgId);
        } else {
            deadLetter(msgId);
        }
    }

    public void failWithRedelivery(Long msgId) {
        try {
            channel.basicReject(msgId, true);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to fail with redelivery", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("could not fail with redelivery for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void deadLetter(Long msgId) {
        try {
            channel.basicReject(msgId, false);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to fail with no redelivery", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("could not fail with dead-lettering (when configured) for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }
}
