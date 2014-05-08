package com.ss.mqtt;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.*;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.slf4j.Logger;

import java.net.URISyntaxException;
import java.util.concurrent.BlockingQueue;

public class MessageConsumer {
    private Logger logger;

    private CallbackConnection connection;

    private String url;

    private BlockingQueue<Message> messages;

    private String queueName;

    private boolean trace = false;

    public MessageConsumer(Logger logger, String url, BlockingQueue<Message> messages, String queueName) {
        this.logger = logger;
        this.url = url;
        this.messages = messages;
        this.queueName = queueName;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    private enum State {
        INIT,
        CONNECTED,
        TOPIC_CONNECTED,
        DISCONNECTED,
    }

    private State state = State.INIT;

    public void open() {
        MQTT mqtt = new MQTT();

        try {
            mqtt.setHost(url);
        } catch (URISyntaxException e) {
            String msg = "Invalid URL for the MQTT Broker";
            logger.error(msg, e);
            throw new RuntimeException(msg, e);
        }

        if (trace) {
            mqtt.setTracer(new Tracer() {
                @Override
                public void onReceive(MQTTFrame frame) {
                    logger.info("recv: " + frame);
                }

                @Override
                public void onSend(MQTTFrame frame) {
                    logger.info("send: " + frame);
                }

                @Override
                public void debug(String message, Object... args) {
                    logger.info(String.format("debug: " + message, args));
                }
            });
        }

        connection = mqtt.callbackConnection();
        // Start add a listener to process subscription messages, and start the
        // resume the connection so it starts receiving messages from the socket.
        connection.listener(new Listener() {
            public void onConnected() {
                logger.debug("connected");
            }

            public void onDisconnected() {
                logger.debug("disconnected");
            }

            public void onPublish(UTF8Buffer topic, Buffer payload, Runnable onComplete) {
                Message message = new Message(payload, topic.toString());
                try {
                    messages.put(message);
                } catch (InterruptedException e) {
                    logger.error("Failed to put the message to queue", e);
                }
                onComplete.run();
            }

            public void onFailure(Throwable value) {
                logger.warn("Connection failure: {}", value);
                connection.disconnect(null);
                state = State.DISCONNECTED;
            }
        });

        connection.connect(new Callback<Void>() {
            public void onFailure(Throwable value) {
                state = State.INIT;
                String s = "Failed to connect to the broker";
                logger.error(s, value);
                throw new RuntimeException(s, value);
            }

            // Once we connect..
            public void onSuccess(Void v) {
                // Subscribe to a topic
                Topic[] topics = {new Topic(queueName, QoS.AT_LEAST_ONCE)};
                connection.subscribe(topics, new Callback<byte[]>() {
                    public void onSuccess(byte[] qoses) {
                        // The result of the subcribe request.
                        logger.debug("Subscribed to the topic {}", queueName);
                        state = State.TOPIC_CONNECTED;
                    }

                    public void onFailure(Throwable value) {
                        logger.error("Failed to subscribe to topic", value);
                        connection.disconnect(null);
                        state = State.DISCONNECTED;
                    }
                });
            }
        });
    }

    public void close() {
        if (connection != null && (state == State.CONNECTED || state == State.TOPIC_CONNECTED)) {
            // To disconnect..
            connection.disconnect(new Callback<Void>() {
                public void onSuccess(Void v) {
                    // called once the connection is disconnected.
                    state = State.DISCONNECTED;
                }
                public void onFailure(Throwable value) {
                    // Disconnects never fail.
                    state = State.DISCONNECTED;
                }
            });
        }
    }
}
