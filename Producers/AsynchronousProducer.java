package Producers;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class AsynchronousProducer {
    public static void main(String[] args) throws Exception {
        String topicName = "AsynchronousProducerTopic";
        String key = "message";
        String value = "I am Saman Munikar.";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
        producer.send(record, new MyProducerCallback());
        producer.close();

        System.out.println("SimpleProducer Completed.");
    }

    static class MyProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                System.out.println("Exception occurred.");
            } else {
                System.out.println("Asynchronous Producer call Success.");
            }
        }
    }
}
