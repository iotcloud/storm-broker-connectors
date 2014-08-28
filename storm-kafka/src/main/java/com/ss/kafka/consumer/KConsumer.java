package com.ss.kafka.consumer;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.ss.commons.DestinationConfiguration;
import com.ss.commons.MessageContext;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;

public class KConsumer {
    private static Logger LOG = LoggerFactory.getLogger(KConsumer.class);

    public static class MessageAndRealOffset {
        public Message msg;
        public long offset;

        public MessageAndRealOffset(Message msg, long offset) {
            this.msg = msg;
            this.offset = offset;
        }
    }

    public static enum EmitState {
        EMITTED_MORE_LEFT,
        EMITTED_END,
        NO_EMITTED
    }

    private BlockingQueue<MessageContext> messageContexts;
    private boolean run = true;
    private String _uuid = UUID.randomUUID().toString();

    ConsumerConfig _consumerConfig;
    PartitionCoordinator _coordinator;
    DynamicPartitionConnections _connections;
    ZkState _state;

    long _lastUpdateMs = 0;
    int _currPartitionIndex = 0;

    String _site;

    int _totalTasks;
    int _taskIndex;

    private Map<String, String> properties;

    public KConsumer(DestinationConfiguration destination, BlockingQueue<MessageContext> messageContexts, int totalTasks, int taskIndex, Map<String, String> properties) {
        this.properties = properties;
        this._site = destination.getSite();
        this._consumerConfig = getConsumerConfig(destination);
        this.messageContexts = messageContexts;
        this._totalTasks = totalTasks;
        this._taskIndex = taskIndex;
    }

    private ConsumerConfig getConsumerConfig(DestinationConfiguration destination) {
        destination.getUrl();
        String queue;
        String zkHost = properties.get("broker.zk.servers");
        String zkRoot = properties.get("broker.zk.root");
        ZkHosts zkHosts = new ZkHosts(zkHost, zkRoot);
        if (!destination.isGrouped()) {
            queue = destination.getSite() + "." + destination.getSensor() + "." + destination.getSensorId() + "." + destination.getProperty("topic");
        } else {
            queue = destination.getSite() + "." + destination.getSensor() + "." + destination.getProperty("topic");
        }
        ConsumerConfig consumerConfig = new ConsumerConfig(zkHosts, queue, "/iot/broker", queue);
        Iterable<String> iterable = Splitter.on(",").split(zkHost);
        Iterator<String> it = iterable.iterator();
        consumerConfig.zkServers = new ArrayList<String>();
        while (it.hasNext()) {
            consumerConfig.zkServers.add(it.next());
        }
        return consumerConfig;
    }

    private void close() {
        run = false;
        _state.close();
    }

    private void nextTuple() {
        List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
        for (int i = 0; i < managers.size(); i++) {
            try {
                // in case the number of managers decreased
                _currPartitionIndex = _currPartitionIndex % managers.size();
                EmitState state = managers.get(_currPartitionIndex).next(messageContexts);
                if (state != EmitState.EMITTED_MORE_LEFT) {
                    _currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
                }
                if (state != EmitState.NO_EMITTED) {
                    break;
                }
            } catch (FailedFetchException e) {
                LOG.warn("Fetch failed", e);
                _coordinator.refresh();
            }
        }

        long now = System.currentTimeMillis();
        if ((now - _lastUpdateMs) > _consumerConfig.stateUpdateIntervalMs) {
            commit();
        }
    }

    private void deactivate() {
        commit();
    }

    private void commit() {
        _lastUpdateMs = System.currentTimeMillis();
        for (PartitionManager manager : _coordinator.getMyManagedPartitions()) {
            manager.commit();
        }
    }

    public class Worker implements Runnable {
        @Override
        public void run() {
            while (run) {
                nextTuple();
            }
        }
    }

    public void start() {
        List<String> zkServers = _consumerConfig.zkServers;
        String servers = Joiner.on(",").join(zkServers);
        _state = new ZkState(servers, _consumerConfig.zkRoot);
        _connections = new DynamicPartitionConnections(_consumerConfig, KafkaUtils.makeBrokerReader(_consumerConfig));

        // using TransactionalState like this is a hack
        _coordinator = new ZkCoordinator(_connections, _consumerConfig, _state, _taskIndex, _totalTasks, _uuid, _site);

        Thread t = new Thread(new Worker());
        t.start();
    }

    public void stop() {
        deactivate();
        run = false;
        close();
    }
}
