package Producers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SupplierConsumer {
    public static void main(String[] args) throws Exception{
        String topicName = "SupplierTopic";
        String groupName = "SupplierTopicGroup";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "Producers.SupplierDeserializer");

        KafkaConsumer<String, Supplier> consumer = null;
        try {
            consumer = new KafkaConsumer<String, Supplier>(props);
            consumer.subscribe(Arrays.asList(topicName));

            while (true) {
                ConsumerRecords<String, Supplier> records = consumer.poll(100);
                for (ConsumerRecord<String, Supplier> record : records) {
                    System.out.println("Supplier id = " + String.valueOf(record.value().getSupplierId())
                            + "Supplier Name = " + record.value().getSupplierName()
                            + "Supplier Date = " + String.valueOf(record.value().getSupplierStartDate()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
