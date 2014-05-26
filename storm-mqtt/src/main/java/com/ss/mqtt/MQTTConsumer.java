package com.ss.mqtt;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.*;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.slf4j.Logger;

import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

public class MQTTConsumer {
    private Logger logger;

    private CallbackConnection connection;

    private String url;

    private BlockingQueue<MQTTMessage> messages;

    private String queueName;

    private boolean trace = false;

    private QoS qoS;

    private String host;

    private int port = -1;

    public MQTTConsumer(Logger logger, String url, BlockingQueue<MQTTMessage> messages, String queueName) {
        this(logger, url, messages, queueName, QoS.AT_MOST_ONCE);
    }

    public MQTTConsumer(Logger logger, String url, BlockingQueue<MQTTMessage> messages, String queueName, QoS qoS) {
        this.logger = logger;
        this.url = url;
        this.messages = messages;
        this.queueName = queueName;
        this.qoS = qoS;

        if (url.contains(":")) {
            this.host = url.substring(0, url.indexOf(":"));
            this.port = Integer.parseInt(url.substring(url.indexOf(":") + 1));
        } else {
            this.host = url;
        }
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
            if (port == -1) {
                mqtt.setHost(url);
            } else {
                mqtt.setHost(host, port);
            }
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
                final String uuid = UUID.randomUUID().toString();
                MQTTMessage message = new MQTTMessage(uuid, payload, topic.toString(), onComplete);
                try {
                    messages.put(message);
                } catch (InterruptedException e) {
                    logger.error("Failed to put the message to queue", e);
                }
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
                state = State.CONNECTED;
                // Subscribe to a topic
                Topic[] topics = {new Topic(queueName, qoS)};
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

    public void ack(MQTTMessage msg) {
        msg.getOnComplete().run();
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
