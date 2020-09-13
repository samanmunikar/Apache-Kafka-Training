package Consumers;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RebalanceListner implements ConsumerRebalanceListener {
    private KafkaConsumer consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffset = new HashMap<>();

    public RebalanceListner(KafkaConsumer cons) {
        this.consumer = cons;
    }

    public void addOffset(String topic, int partition, long offset) {
        currentOffset.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffset() {
        return Collections.unmodifiableMap(currentOffset);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Revoked : ");
        for(TopicPartition partition : partitions) {
            System.out.println(partition.partition());
        }

        System.out.println("Following Partitions commited : ");
        for(TopicPartition tp: currentOffset.keySet()) {
            System.out.println(tp.partition());
        }

        consumer.commitSync(currentOffset);
        currentOffset.clear();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Assigned : ");
        for(TopicPartition partition : partitions) {
            System.out.println(partition.partition()+ ",");
        }
    }
}
