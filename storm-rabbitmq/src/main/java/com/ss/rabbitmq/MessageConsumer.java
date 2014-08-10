package com.ss.rabbitmq;

import com.rabbitmq.client.*;
import com.ss.commons.DestinationConfiguration;
import com.ss.commons.MessageContext;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class MessageConsumer {
    private Connection connection;

    private Channel channel;

    private QueueingConsumer consumer;

    private String consumerTag;

    private ErrorReporter reporter;

    private DestinationConfiguration destination;

    private Logger logger;

    private BlockingQueue<MessageContext> messages;

    private String exchangeName;

    private String queue;

    private String routingKey;

    private int prefetchCount;

    private boolean isReQueueOnFail = false;

    private boolean autoAck = true;

    public MessageConsumer(BlockingQueue<MessageContext> messages, DestinationConfiguration destination,
                           ErrorReporter reporter, Logger logger, int prefetchCount, boolean isRequeueOnFail, boolean autoAck) {
        this.destination = destination;
        this.reporter = reporter;
        this.logger = logger;
        this.messages = messages;
        this.prefetchCount = prefetchCount;
        this.isReQueueOnFail = isRequeueOnFail;
        this.autoAck = autoAck;

        readProps();
    }

    private void readProps() {
        queue = destination.getProperty("queue");
        if (queue == null) {
            String msg = "The property queue must be specified";
            logger.error(msg);
            throw new RuntimeException(msg);
        }

        routingKey = destination.getProperty("routingKey");
        exchangeName = destination.getProperty("exchange");
    }

    private void reset() {
        consumerTag = null;
    }

    private void reInitIfNecessary() {
        if (consumerTag == null || consumer == null) {
            close();
            openConnection();
        }
    }

    public void close() {
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
    }

    public void openConnection() {
        try {
            connection = createConnection();
            channel = connection.createChannel();

            if (prefetchCount > 0) {
                logger.info("setting basic.qos / prefetch count to " + prefetchCount + " for " + queue);
                channel.basicQos(prefetchCount);
            }

            if (routingKey != null && exchangeName != null) {
                channel.exchangeDeclare(exchangeName, "direct", false);
                channel.queueDeclare(queue, true, false, false, null);
                channel.queueBind(queue, exchangeName, routingKey);
            }
            consumer = new QueueingConsumer(channel);
            consumerTag = channel.basicConsume(queue, autoAck, new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    MessageContext message = new MessageContext(Long.toString(envelope.getDeliveryTag()), destination.getName(), new RabbitMQMessage(queue, consumerTag, envelope, properties, body));
                    try {
                        messages.put(message);
                    } catch (InterruptedException e) {
                        reporter.reportError(e);
                    }
                }
            });
        } catch (Exception e) {
            reset();
            logger.error("could not open listener on queue " + queue);
            reporter.reportError(e);
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
