package com.ss.mqtt;

import org.fusesource.mqtt.client.*;
import org.fusesource.mqtt.codec.MQTTFrame;
import org.slf4j.Logger;

import java.net.URISyntaxException;

public class MQTTProducer {
    private Logger logger;

    private CallbackConnection connection;

    private String url;

    private String queueName;

    private boolean trace = false;

    private QoS qoS;

    private String host;

    private int port;

    public MQTTProducer(Logger logger, String url, String queueName) {
        this(logger, url, queueName, QoS.AT_MOST_ONCE);
    }

    public MQTTProducer(Logger logger, String url, String queueName, QoS qoS) {
        this.logger = logger;
        this.url = url;
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

        connection.getDispatchQueue().execute(new Runnable() {
            @Override
            public void run() {
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
                    }
                });
            }
        });
    }

    public void send(final byte []message) {
        if (connection != null && state == State.CONNECTED) {
            connection.getDispatchQueue().execute(new Runnable() {
                @Override
                public void run() {
                    connection.publish(queueName, message, qoS, false, null);
                }
            });
        } else {
            logger.warn("Trying to send messages on a closed connection");
        }
    }

    public void ack(MQTTMessage msg) {
        msg.getOnComplete().run();
    }

    public void close() {
        if (connection != null && (state == State.CONNECTED || state == State.TOPIC_CONNECTED)) {
            connection.getDispatchQueue().execute(new Runnable() {
                @Override
                public void run() {
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
            });
        }
    }
}
