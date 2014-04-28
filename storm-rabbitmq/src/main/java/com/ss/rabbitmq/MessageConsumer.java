package com.ss.rabbitmq;

import com.rabbitmq.client.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class MessageConsumer {
    public enum State {
        INIT,
        CONNECTED,
        CLOSED,
        ERROR,
        RESTARTING
    }

    private Connection connection;

    private Channel channel;

    private QueueingConsumer consumer;

    private String consumerTag;

    private ErrorReporter reporter;

    private RabbitMQConfigurator configurator;

    private String queueName;

    private State state = State.INIT;

    private Logger logger;

    private BlockingQueue<Message> messages;

    public MessageConsumer(BlockingQueue<Message> messages, String queueName,
                           RabbitMQConfigurator configurator,
                           ErrorReporter reporter, Logger logger) {
        this.queueName = queueName;
        this.configurator = configurator;
        this.reporter = reporter;
        this.logger = logger;
        this.messages = messages;
    }

    private void reset() {
        consumerTag = null;
    }

    private void reInitIfNecessary() {
        if (consumerTag == null || consumer == null) {
            closeConnection();
            openConnection();
        }
    }

    public void closeConnection() {
        try {
            if (channel != null && channel.isOpen()) {
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
        state = State.CLOSED;
    }

    public void openConnection() {
        try {
            connection = createConnection();
            channel = connection.createChannel();

            if (configurator.getPrefetchCount() > 0) {
                logger.info("setting basic.qos / prefetch count to " + configurator.getPrefetchCount() + " for " + configurator.getQueueName());
                channel.basicQos(configurator.getPrefetchCount());
            }

            consumer = new QueueingConsumer(channel);
            consumerTag = channel.basicConsume(queueName, configurator.isAutoAcking(), new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    Message message = new Message(queueName, consumerTag, envelope, properties, body);
                    try {
                        messages.put(message);
                    } catch (InterruptedException e) {
                        reporter.reportError(e);
                    }
                }
            });

            state = State.CONNECTED;
        } catch (Exception e) {
            state = State.ERROR;
            reset();
            logger.error("could not open listener on queue " + configurator.getQueueName());
            reporter.reportError(e);
        }
    }

    private Connection createConnection() throws IOException {
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setUri(configurator.getURL());
            Connection connection = connectionFactory.newConnection();
            connection.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException cause) {
                    logger.error("shutdown signal received", cause);
                    reporter.reportError(cause);
                    reset();
                }
            });
            logger.info("connected to rabbitmq: " + connection + " for " + configurator.getQueueName());
            return connection;

        } catch (Exception e) {
            logger.info("connected to rabbitmq: " + connection + " for " + configurator.getQueueName());
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
        if (configurator.isReQueueOnFail()) {
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
