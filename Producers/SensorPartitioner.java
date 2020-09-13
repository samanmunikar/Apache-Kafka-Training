package Producers;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class SensorPartitioner implements Partitioner {
    private String speedSensorName;

    @Override
    public void configure(Map<String, ?> configs) {
        speedSensorName = configs.get("speed.sensor.name").toString();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitionList =  cluster.partitionsForTopic(topic);
        int numPartitions = partitionList.size();
        int partitionForSpecificTopic = (int) Math.abs(numPartitions * 0.3);
        int p = 0;

        if ((keyBytes == null) || !(key instanceof String)) {
            throw new InvalidRecordException("All message must have sensor name as the key");
        }

        if (((String) key).equals(speedSensorName)) {
            p = Utils.toPositive(Utils.murmur2(valueBytes)) % partitionForSpecificTopic;
        } else {
            p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - partitionForSpecificTopic) + partitionForSpecificTopic;
        }

        System.out.println("Key = " + (String) key + "Partition = " + p);
        return p;
    }

    @Override
    public void close() {

    }
}
