package com.ss.jms;

import backtype.storm.topology.OutputFieldsDeclarer;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import java.io.Serializable;
import java.util.Map;

public interface JMSConfigurator extends Serializable {
    /**
     * Weather we are automatically acknowledging the messages
     * @return
     */
    int ackMode();

    /**
     * Provides the JMS <code>ConnectionFactory</code>
     * @return the connection factory
     * @throws Exception
     */
    public ConnectionFactory connectionFactory() throws Exception;

    /**
     * Provides the <code>Destination</code> (topic or queue) from which the
     * <code>JmsSpout</code> will receive messages.
     * @return
     * @throws Exception
     */
    public Map<String, Destination> destinations() throws Exception;

    MessageBuilder getMessageBuilder();

    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);

    int queueSize();

    public JMSDestinationSelector getDestinationSelector();
}
