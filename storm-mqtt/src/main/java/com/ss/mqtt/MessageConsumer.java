package com.ss.mqtt;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.*;
import org.slf4j.Logger;

import java.net.URISyntaxException;
import java.util.concurrent.BlockingQueue;

public class MessageConsumer {
    private Logger logger;

    private MQTT mqtt;

    private CallbackConnection connection;

    private String url;

    private BlockingQueue<Message> messages;

    private String queueName;

    public void open() {
        mqtt = new MQTT();

        try {
            mqtt.setHost(url);
        } catch (URISyntaxException e) {

        }

        connection = mqtt.callbackConnection();

        connection.listener(new Listener() {
            public void onDisconnected() {

            }
            public void onConnected() {

            }

            public void onPublish(UTF8Buffer topic, Buffer payload, Runnable ack) {
                // You can now process a received message from a topic.
                // Once process execute the ack runnable.
                ack.run();
            }
            public void onFailure(Throwable value) {
                connection.disconnect(null); // a connection failure occured.
            }
        });

        connection.connect(new Callback<Void>() {
            public void onFailure(Throwable value) {

            }

            // Once we connect..
            public void onSuccess(Void v) {
                // Subscribe to a topic
                Topic[] topics = {new Topic(queueName, QoS.AT_LEAST_ONCE)};
                connection.subscribe(topics, new Callback<byte[]>() {
                    public void onSuccess(byte[] qoses) {
                        // The result of the subcribe request.
                    }
                    public void onFailure(Throwable value) {
                        connection.disconnect(null); // subscribe failed.
                    }
                });

                // To disconnect..
                connection.disconnect(new Callback<Void>() {
                    public void onSuccess(Void v) {
                        // called once the connection is disconnected.
                    }
                    public void onFailure(Throwable value) {
                        // Disconnects never fail.
                    }
                });
            }
        });
    }

    public void close() {

    }
}
