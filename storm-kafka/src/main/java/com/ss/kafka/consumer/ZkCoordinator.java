package com.ss.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.ss.kafka.consumer.KafkaUtils.taskId;

public class ZkCoordinator implements PartitionCoordinator {
    public static final Logger LOG = LoggerFactory.getLogger(ZkCoordinator.class);
    private final String _sensor;

    ConsumerConfig _spoutConfig;
    int _taskIndex;
    int _totalTasks;
    String _topologyInstanceId;
    Map<Partition, PartitionManager> _managers = new HashMap<Partition, PartitionManager>();
    List<PartitionManager> _cachedList;
    Long _lastRefreshTime = null;
    int _refreshFreqMs;
    DynamicPartitionConnections _connections;
    DynamicBrokersReader _reader;
    ZkState _state;

    public ZkCoordinator(DynamicPartitionConnections connections, ConsumerConfig spoutConfig, ZkState state,
                         int taskIndex, int totalTasks, String topologyInstanceId, String sensor) {
        this(connections, spoutConfig, state, taskIndex, totalTasks, topologyInstanceId, buildReader(spoutConfig), sensor);
    }

    public ZkCoordinator(DynamicPartitionConnections connections,
                         ConsumerConfig spoutConfig, ZkState state,
                         int taskIndex, int totalTasks, String topologyInstanceId,
                         DynamicBrokersReader reader, String sensor) {
        _spoutConfig = spoutConfig;
        _connections = connections;
        _taskIndex = taskIndex;
        _totalTasks = totalTasks;
        _topologyInstanceId = topologyInstanceId;
        _state = state;
        ZkHosts brokerConf = (ZkHosts) spoutConfig.hosts;
        _refreshFreqMs = brokerConf.refreshFreqSecs * 1000;
        _reader = reader;
        _sensor = sensor;
    }

    private static DynamicBrokersReader buildReader(ConsumerConfig spoutConfig) {
        ZkHosts hosts = (ZkHosts) spoutConfig.hosts;
        return new DynamicBrokersReader(hosts.brokerZkStr, hosts.brokerZkPath, spoutConfig.topic);
    }

    @Override
    public List<PartitionManager> getMyManagedPartitions() {
        if (_lastRefreshTime == null || (System.currentTimeMillis() - _lastRefreshTime) > _refreshFreqMs) {
            refresh();
            _lastRefreshTime = System.currentTimeMillis();
        }
        return _cachedList;
    }

    @Override
    public void refresh() {
        try {
            LOG.info(taskId(_taskIndex, _totalTasks) + "Refreshing partition manager connections");
            GlobalPartitionInformation brokerInfo = _reader.getBrokerInfo();
            List<Partition> mine = KafkaUtils.calculatePartitionsForTask(brokerInfo, _totalTasks, _taskIndex);

            Set<Partition> curr = _managers.keySet();
            Set<Partition> newPartitions = new HashSet<Partition>(mine);
            newPartitions.removeAll(curr);

            Set<Partition> deletedPartitions = new HashSet<Partition>(curr);
            deletedPartitions.removeAll(mine);

            LOG.info(taskId(_taskIndex, _totalTasks) + "Deleted partition managers: " + deletedPartitions.toString());

            for (Partition id : deletedPartitions) {
                PartitionManager man = _managers.remove(id);
                man.close();
            }
            LOG.info(taskId(_taskIndex, _totalTasks) + "New partition managers: " + newPartitions.toString());

            for (Partition id : newPartitions) {
                PartitionManager man = new PartitionManager(_connections, _topologyInstanceId, _state, _spoutConfig, id, _sensor);
                _managers.put(id, man);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        _cachedList = new ArrayList<PartitionManager>(_managers.values());
        LOG.info(taskId(_taskIndex, _totalTasks) + "Finished refreshing");
    }

    @Override
    public PartitionManager getManager(Partition partition) {
        return _managers.get(partition);
    }
}

