package Producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class SynchronousProducer {
    public static void main(String[] args) {
        String topicName = "Synchronous Producer";
        String key = "message";
        String value = "Learning Synchronous Producer";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);

        try {
            RecordMetadata metadata = producer.send(producerRecord).get();
            System.out.println("Message is sent ot Partition number " + metadata.partition() + " and offset " +
                    metadata.offset());
            System.out.println("Synchronous Producer completed with Success.");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Synchronous Producer failed.");
        } finally {
            producer.close();
        }
    }
}
